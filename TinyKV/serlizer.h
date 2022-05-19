#include<string>
#include "rapidjson/document.h"     
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h"

using namespace rapidjson;


class KVType {
public:
	virtual ~KVType() {};

	virtual void serlize(Writer<StringBuffer>* writer) = 0;

	virtual KVType* deserlize(Value& doc) = 0;
};

class Command {
public:
	virtual ~Command() {};
	virtual const char* serlizeCommand() = 0;
};

template<typename K, typename V>
class CommandSet: public Command{
public:
	CommandSet(K* key, V* val) :key(key), value(val), sb(StringBuffer{}),writer(Writer<StringBuffer>(sb)) {
	}

	CommandSet(K* key, V* val, Document& doc): CommandSet(key, val) {
		Value& kdoc = doc["key"];
		Value& vdoc = doc["value"];
		key->deserlize(kdoc);
		val->deserlize(vdoc);		
	}

	K* getKey() {
		return this->key;
	}

	V* getValue() {
		return this->value;
	}
	static CommandSet<K,V>* fromStr(const char* json_str) {
		Document doc;
		doc.Parse(json_str);
		if (doc["cmd"] == "Set") {
			K key{};
			V val{};
			CommandSet<K, V>* cmd = new CommandSet<K, V>(&key, &val, doc);
			return cmd;
		}
		return nullptr;
	}

	const char* serlizeCommand() {
		writer.StartObject();
		writer.String("cmd");
		writer.String("Set");
		writer.String("key");
		key->serlize(&writer);
		writer.String("value");
		value->serlize(&writer);
		writer.EndObject();
		
		return sb.GetString();
	}
private:
	K* key;
	V* value;
	StringBuffer sb;
	Writer<StringBuffer> writer;

};

template<typename K>
class CommandRemove : public Command {
public:
	CommandRemove(K* key) :key(key), sb(StringBuffer{}),writer(Writer<StringBuffer>(sb)) {
	}

	CommandRemove(K* key, Document& doc) : CommandRemove(key) {
		Value& kdoc = doc["key"];
		key->deserlize(kdoc);
	}

	K* getKey() {
		return this->key;
	}
	static CommandRemove<K>* fromStr(const char* json_str) {
		Document doc;
		doc.Parse(json_str);
		if (doc["cmd"] == "Remove") {
			K key{};
			CommandRemove<K>* cmd = new CommandRemove<K>(&key, doc);
			return cmd;
		}
		return nullptr;
	}
	const char* serlizeCommand() {
		writer.StartObject();
		writer.String("cmd");
		writer.String("Remove");
		writer.String("key");
		key->serlize(&writer);
		writer.EndObject();

		return sb.GetString();
	}
private:
	K* key;
	StringBuffer sb;
	Writer<StringBuffer> writer;

};

template<typename K, typename V>
Command* deserlize(const char* json_str) {
	Document doc;
	doc.Parse(json_str);
	if (doc["cmd"] == "Set") {
		K key{};
		V val{};
		Command* cmd = new CommandSet<K,V>(&key, &val, doc);
		return cmd;
	}
	else if (doc["cmd"] == "Remove") {
		K key{};
		Command* cmd = new CommandRemove<K>(&key, doc);
		return cmd;
	}
	return nullptr;
}

struct CommandPos {
	unsigned long gen;
	std::streampos pos;
	unsigned long len;
	CommandPos() {}
	CommandPos(unsigned long gen, std::streampos pos, unsigned long len):gen(gen),pos(pos),len(len) {}
};