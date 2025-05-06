#include "./lru_cache.h"
#include "../logger/logger.h"
#include <stdexcept>
#include <utility>

char * GFSLRUCache::get(int write_id) {

	if (auto find_result = map.find(write_id); find_result != map.end()){
		list.splice(list.begin(), list, find_result->second);
		return list.begin()->second;
	}
	return nullptr;
};

void GFSLRUCache::put(int write_id) {
	if (auto find_result = map.find(write_id);  find_result == map.end()) {
		list.push_front(std::make_pair(write_id, new char[buffer_size_]));
		map[write_id] = list.begin();
		if (list.size() > capacity_) {
			delete list.back().second;
			map.erase(list.back().first);
			list.pop_back();
		}
		return;
	}
	throw std::runtime_error("impossible of the same write id to be used twice");
};


void GFSLRUCache::evict(int write_id) {
	if (auto find_result = map.find(write_id); find_result != map.end()) {
		delete find_result->second->second;
		list.erase(find_result->second);
		map.erase(find_result->second->first);
	}

	throw std::runtime_error("write id doesn't exist");
}
/*BUFFER POOL*/
