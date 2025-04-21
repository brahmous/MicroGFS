#ifndef GFS_LRU_CACHE_H
#define GFS_LRU_CACHE_H

#include <list>
#include <unordered_map>

class GFSLRUCache {
public:
  GFSLRUCache(std::size_t capacity, std::size_t buffer_size)
      : capacity_{capacity}, buffer_size_{buffer_size} {};

  char *get(int write_id);

  void put(int write_id);

	void evict(int write_id);

private:
  std::unordered_map<int, std::list<std::pair<int, char *>>::iterator> map;
  std::list<std::pair<int, char *>> list;
  std::size_t capacity_;
  std::size_t buffer_size_;
};

#endif
