#include<Windows.h>
#include<iostream>
#include<sstream>
#include<fstream>
#include<unordered_map>
#include<string>
#include<atomic>
#include<memory>
#include<mutex>
#include<functional>
#include <filesystem>
#include"serlizer.h"
#include"engine.h"
#include"skiplist.h"
#include"cache.h"

namespace fs = std::filesystem;

# define BUF_SIZE 1000
# define SKIP_LIST_MAX_LEVEL 6 
const unsigned long COMPACTION_THRESHOLD = 1024 * 1024 * 4;
std::mutex mutex;

std::string log_path(std::string path, unsigned long gen) {
	path += "\\";
	path += std::to_string(gen);
	path += ".log";
	return path;
}

std::vector<fs::path> load_log(std::string dirPath) {
	std::vector<fs::path> logs{};
	for (auto& entry : fs::directory_iterator(dirPath)) {
		if (entry.path().extension()== fs::path(".log")) {
			logs.push_back(entry.path());
		}
	}
	return logs;
}

unsigned long last_gen(std::vector<fs::path>& logs) {
	unsigned long last = 0;
	for (fs::path logPath : logs) {
		unsigned long gen= std::stoi(logPath.stem().string());
		if (gen > last) {
			last = gen;
		 }
	}
	return last;
}

class BufReaderWithPos {
public:
	BufReaderWithPos(fs::path path){
		reader = std::ifstream(path);
		pos = 0;
		reader.seekg(0, reader.end);
		length = reader.tellg();
		reader.seekg(0);
	}

	~BufReaderWithPos() {
		reader.close();
	}

	int read(char buf[], unsigned long bufSize) {
		unsigned long remain = length - pos;
		if (remain == 0)return 0;
		unsigned long readlen = 0;
		if (remain >= bufSize) {
			readlen = bufSize - 1;
		}
		else {
			readlen = remain;
		}
		reader.read(buf, readlen);
		buf[readlen] = '\0';
		pos += readlen;
		return readlen;
	}

	std::string read_all() {
		std::stringstream strStream;
		strStream << reader.rdbuf();		
		std::string txt = strStream.str();
		return txt;
	}

	void seek(std::streampos pos) {
		reader.seekg(pos);
	}
private:
	std::ifstream reader;
	unsigned long pos;
	unsigned long length;

};

class BufWriterWithPos {
public:
	BufWriterWithPos(fs::path path) {
		this->writer = std::ofstream(path, std::ios::app);
		this->writer.seekp(0, writer.end);
		this->pos = writer.tellp();
	}

	~BufWriterWithPos() {
		writer.close();
	}

	void write(const char *buf, unsigned long bufsize) {
		writer.write(buf, bufsize);
		writer.seekp(0, writer.end);
		pos = writer.tellp();
	}

	void seek(std::streampos pos) {
		writer.seekp(pos);
	}

	std::streampos currentPos() {
		return pos;
	}

private:
	std::ofstream writer;
	std::streampos pos;
};

class KVStoreReader {
public:
	KVStoreReader(std::string dirpath, 
		unsigned long safe_point):
		dirpath(dirpath), 
		safe_point(std::atomic<unsigned long>(safe_point))
	{
		this->readers = std::unordered_map<unsigned long, BufReaderWithPos*>();
		this->buf = (char*)malloc(sizeof(char) * BUF_SIZE);
	}

	~KVStoreReader() {
		free(this->buf);
	}
	template<typename K, typename V>
	unsigned long loadDataFromLog(std::shared_ptr<SkipList<K, CommandPos>> index,std::vector<fs::path>&& logs) {
		unsigned long uncompacted = 0;
		for (fs::path logPath : logs) {
			BufReaderWithPos* bufReader = new BufReaderWithPos(logPath);

			bufReader->seek(std::streampos(0));
			std::string txt = bufReader->read_all();
			std::streampos pos = 0;
			std::size_t delimiterPos;
			while ((delimiterPos = txt.find("#")) != std::string::npos) {
				std::string token = txt.substr(0, delimiterPos);
				txt.erase(0, delimiterPos + 1);

				uncompacted += token.length();
				try {
					CommandSet<K, V>* cmdset = CommandSet<K, V>::fromStr(token.c_str());
					if (cmdset != nullptr) {
						CommandPos cmdpos(std::stoi(logPath.stem()), pos, delimiterPos);
						index->insert_element(*(cmdset->getKey()), cmdpos);
					}
					else {
						CommandRemove<K>* cmdrm = CommandRemove<K>::fromStr(token.c_str());
						if (cmdrm != nullptr) {
							index->delete_element(*(cmdrm->getKey()));
						}
					}					
				}
				catch (...) {
				}
				pos += delimiterPos;
				pos += 1;
			}
			this->readers.insert(std::make_pair(std::stoi(logPath.stem().string()), bufReader));
		}
		return uncompacted;
	}

	void closeStaleHandles() {		
		auto it = readers.begin();
		while (it != readers.end()) {
			if (it->first < this->safe_point.load(std::memory_order::memory_order_seq_cst)) {
				delete it->second;
				it = readers.erase(it);
			}
			else {
				++it;
			}
		}
	}

	void insertLogHandle(unsigned long gen) {
		BufReaderWithPos* bufReader = new BufReaderWithPos(log_path(this->dirpath, gen));
		this->readers.insert(std::make_pair(gen, bufReader));
	}

	void updateSafePoint(unsigned long compaction_gen) {
		safe_point.store(compaction_gen, std::memory_order::memory_order_seq_cst);
	}

	template<typename R>
	R* read_and(const unsigned long gen, 
		std::streampos pos, unsigned long len, 
		std::function<R* (const char*, unsigned long)> f) 
	{
		closeStaleHandles();

		auto it = this->readers.find(gen); 
		if (it == this->readers.end()) {
			return nullptr;
		}		
		(it->second)->seek(pos);
		if (len > BUF_SIZE) {
			free(buf);
			buf = (char*)malloc(sizeof(char) * (len+1));
		}
		(it->second)->read(buf, len+1);
		return f(buf, len);		
	}

	void read_and(const unsigned long gen,
		std::streampos pos, unsigned long len,
		std::function<void(const char*, unsigned long)> f)
	{
		closeStaleHandles();

		auto it = readers.find(gen);
		if (it == readers.end()) {
			return;
		}
		(it->second)->seek(pos);
		if (len > BUF_SIZE) {
			free(buf);
			buf = (char*)malloc(sizeof(char) * (len + 1));
		}
		(it->second)->read(buf, len + 1);
		f(buf, len);
	}
	template<typename K, typename V>
	Command* read_command(const unsigned long gen, 
		std::streampos pos, 
		unsigned long len) {

		return read_and<Command>(gen, pos, len, 
			[](const char* buf, unsigned long len)-> Command* {
				return deserlize<K,V>(buf);
			});
	}

private:
	std::string dirpath;
	std::atomic<unsigned long> safe_point;
	std::unordered_map<unsigned long, BufReaderWithPos*> readers;
	char* buf;
};

template<typename K, typename V>
class KVStoreWriter {
public:
	KVStoreWriter(std::string dir_path,
		unsigned long current_gen,
		unsigned long uncompacted,
		std::shared_ptr<KVStoreReader> kvsreader,
		BufWriterWithPos* writer,
		std::shared_ptr<SkipList<K, CommandPos>> index) 
	{
		this->dir_path = std::move(dir_path);
		this->kvsreader = kvsreader;
		this->writer = std::unique_ptr<BufWriterWithPos>(writer);
		this->index = index;
		this->current_gen = current_gen;
		this->uncompacted = uncompacted;

	}

	void set(K* key, V* value) {
		std::streampos start = writer->currentPos();
		CommandSet<K,V> cmdset(key, value);
		const char* log_entry = cmdset.serlizeCommand();
		writer->write(log_entry, strlen(log_entry));
		writer->write("#", 1);
		CommandPos cmdpos(this->current_gen, start, strlen(log_entry));
		CommandPos* old_cmdpos = this->index->search_element(*key);
		if (old_cmdpos != nullptr) {
			this->uncompacted += old_cmdpos->len;
		}
		this->index->insert_element(*key, cmdpos);
		this->uncompacted += strlen(log_entry);
		
		if (this->uncompacted > COMPACTION_THRESHOLD) {
			compact();
		}
	}

	void remove(K* key) {
		CommandPos* cmdpos = this->index->search_element(*key);
		if (cmdpos == nullptr)return;

		std::streampos start = writer->currentPos();
		CommandRemove<K> cmdrm(key);
		const char* log_entry = cmdrm.serlizeCommand();
		writer->write(log_entry, strlen(log_entry));
		writer->write("#", 1);
		this->uncompacted += cmdpos->len;
		this->index->delete_element(*key);
		this->uncompacted += (this->writer->currentPos() - start);

		if (this->uncompacted > COMPACTION_THRESHOLD) {
			compact();
		}
	}

	void compact() {
		BufWriterWithPos* w = this->writer.release();
		delete w;
		this->kvsreader->insertLogHandle(this->current_gen);
		unsigned long compaction_gen = this->current_gen + 1;
		this->current_gen += 2;
		this->writer =  std::unique_ptr<BufWriterWithPos>(new BufWriterWithPos(log_path(this->dir_path, this->current_gen)));

		BufWriterWithPos* compaction_writer = new BufWriterWithPos(log_path(this->dir_path, compaction_gen));
		
		std::streampos newpos = 0;
		typename SkipList<K,CommandPos>::Iterator it = this->index->begin();
		for (it++; it != index->end(); it++) {
			CommandPos* cmdpos = (*it)->get_value();
			this->kvsreader->read_and(cmdpos->gen, cmdpos->pos, cmdpos->len, [&compaction_writer](const char* buf, unsigned long len) {
				compaction_writer->write(buf, len);
				});
			compaction_writer->write("#", 1);
			cmdpos->gen = compaction_gen;
			cmdpos->pos = newpos;
			newpos += cmdpos->len;
			newpos += 1;
		}
		this->kvsreader->updateSafePoint(compaction_gen);
		this->kvsreader->closeStaleHandles();
		this->uncompacted = 0;

		std::vector<fs::path> logs = load_log(this->dir_path);
		for (fs::path logPath : logs) {
			unsigned long gen = std::stoi(logPath.stem().string());
			if (gen < compaction_gen) {
				fs::remove(logPath);
			}
		}
	}

private:
	std::string dir_path;
	unsigned long current_gen;
	unsigned long uncompacted;
	std::shared_ptr<KVStoreReader> kvsreader;
	std::unique_ptr<BufWriterWithPos> writer;
	std::shared_ptr<SkipList<K, CommandPos>> index;

};

template<typename K, typename V>
class KVStore{
public:
	KVStore(std::string dir_path) {
		fs::create_directory(dir_path);
		
		std::vector<fs::path> logs = load_log(dir_path);
		unsigned long current_gen = last_gen(logs) + 1;
		BufWriterWithPos* bufwriter = new BufWriterWithPos(log_path(dir_path, current_gen));

		this->path = std::move(dir_path);
		this->index = std::make_shared<SkipList<K, CommandPos>>(SKIP_LIST_MAX_LEVEL);
		this->reader = std::make_shared<KVStoreReader>(this->path, 1);
		unsigned long uncompacted = this->reader->loadDataFromLog<K, V>(this->index, std::move(logs));
		
		this->writer = std::make_unique<KVStoreWriter<K, V>>(this->path, current_gen, uncompacted, this->reader, bufwriter, this->index);		
	}

	void set(K* key, V* value) {
		mutex.lock();
		this->writer->set(key, value);
		mutex.unlock();
	}

	V* get(K* key) {
		CommandPos* cmdpos = this->index->search_element(*key);
		if (cmdpos == nullptr)return nullptr;
		Command* cmd = this->reader->read_command<K,V>(cmdpos->gen, cmdpos->pos, cmdpos->len);
		CommandSet<K, V>* cmdset = dynamic_cast<CommandSet<K, V>*>(cmd);
		if (cmdset != nullptr) {
			return cmdset->getValue();
		}
		return nullptr;
	}

	void remove(K* key) {
		mutex.lock();
		this->writer->remove(key);
		mutex.unlock();
	}

private:
	std::string path;
	std::shared_ptr<SkipList<K, CommandPos>> index;
	std::shared_ptr<KVStoreReader> reader;
	std::unique_ptr<KVStoreWriter<K, V>> writer;
};

