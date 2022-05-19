#include<atomic>
#include<vector>
#include<cassert>
#define BLOCK_SIZE 1024 * 1024 * 4

typedef unsigned char* RowPtr;

class BlockArena {
public:
	~BlockArena() {
		for (auto ptr : blocks) {
			free(ptr);
		}
	}

	RowPtr allocate_new_block(unsigned long block_bytes) {
		RowPtr row_ptr = (RowPtr)malloc(block_bytes);
		blocks.push_back(row_ptr);
		memory_usage.fetch_add(block_bytes, std::memory_order::memory_order_relaxed);
		return row_ptr;
	}

	RowPtr allocate_fallback(unsigned long size) {
		if (size > (BLOCK_SIZE / 4)) {
			return allocate_new_block(size);
		}
		RowPtr new_block_ptr = allocate_new_block(BLOCK_SIZE);
		RowPtr new_ptr = new_block_ptr + size;
		ptr.store(new_ptr, std::memory_order::memory_order_release);
		bytes_remaining.store(BLOCK_SIZE - size, std::memory_order::memory_order_release);
		return new_block_ptr;
	}

	void* allocate(size_t chunk, size_t align) {
		assert((align & (align - 1)) == 0, "\'align\' should be a power(2).");
		size_t slop;
		size_t current_mod = reinterpret_cast<size_t>(ptr.load(std::memory_order::memory_order_acquire)) & (align - 1);
		if (current_mod == 0) {
			slop = 0;
		}
		else{
			slop = align - current_mod;
		}

		RowPtr result = nullptr;
		if (chunk + slop> bytes_remaining.load(std::memory_order::memory_order_acquire)) {
			result = allocate_fallback(chunk);
		}
		else {
			result = ptr.load(std::memory_order::memory_order_acquire)+slop;
			ptr.store(result + chunk, std::memory_order::memory_order_release);
			bytes_remaining.fetch_sub(chunk + slop, std::memory_order::memory_order_seq_cst);
		}
		int ptr_sie = sizeof(RowPtr);
		assert((reinterpret_cast<unsigned long>(result) & (align - 1)) == 0, "allocated memory should be aligned with {}", ptr_size);
		return reinterpret_cast<void*>(result);
	}

	inline unsigned long memory_used(){
		return memory_usage.load(std::memory_order::memory_order_acquire);
	}

private:
	std::atomic<RowPtr> ptr;
	std::atomic<unsigned long> bytes_remaining;
	std::vector<RowPtr> blocks;
	std::atomic<unsigned long> memory_usage;
};