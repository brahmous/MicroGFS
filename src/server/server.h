#ifndef GFS_SERVER_H
#define GFS_SERVER_H
#include <queue>
struct chunk_info {
	std::queue<int> write_queue;
	bool leased = false;
};
#endif
