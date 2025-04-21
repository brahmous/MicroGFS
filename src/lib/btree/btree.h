#ifndef BTREE_H
#define BTREE_H

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <cmath>
#include <memory>
#include <ranges>
#include <stdexcept>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace BTREE {

enum class KeyPosition{
    PRE,
   	POST 
};

template <typename K, typename T, size_t N>
struct Btree {
	//////////////////////////////////////////////
	/// Internal node definition.
	struct Node; 
	using key_list = typename std::vector<K>; 
	using pointer_list = typename std::vector<std::shared_ptr<Node>>;
	using record_list = typename std::vector<T>; 

	struct Node 
	{
		// TODO: Think about sfinae constructors or a factory function.
		static_assert(N >= 2, "B+-tree order N must be greater than 2");

		static constexpr size_t MaxKeys = N - 1;
		static constexpr size_t SplitPos = std::ceil(N/2);
		std::weak_ptr<Node> next;

		// NodeType type;
		key_list keys;
		std::variant<std::monostate, pointer_list, record_list> children;
		
		bool is_leaf() const noexcept
		{
			return std::holds_alternative<record_list>(children);
		}

		size_t key_size() const noexcept
		{
			return keys.size();
		}
		
		Node() = default;

		Node(const Node&) = delete;
		Node& operator=(const Node&) = delete;

		Node(Node &&) = default;
		Node& operator=(Node &&) = default;

		~Node() = default;


		void insert(K key, T record, size_t position) 
		{}

		void insert(K key, std::shared_ptr<Node> in_node, size_t position) 
		{}

		friend std::ostream& operator<<(std::ostream& out, const typename Btree<K, T, N>::Node& node) { 
			for (size_t i=0; i<node.keys.size(); ++i){
				out << "K(" << i << ") = " << node.keys[i] << (i == node.keys.size()-1 ? ".\n" : ", "); 
			}
			const record_list& recs = std::get<record_list>(node.children);
			for (size_t i=0; i<recs.size(); ++i) {
				std::cout << "P("<< i << ") = [" << recs.at(i) << "]" << (i == node.keys.size()-1 ? ".\n" : ", ");
			}
			return out;
		}
	};

	struct search_descriptor {
		bool exact_match = true;
		bool collect_breadcrumbs = false;
		std::shared_ptr<Node> leaf;
		size_t position;
		std::unordered_map<
			std::shared_ptr<Node>,
			std::pair<size_t, std::shared_ptr<Node>>> // position in parent, parent.
			bread_crumbs;

		friend std::ostream& operator << (std::ostream& out, const search_descriptor& des) {
			return out;
		}
	};

	void binary_search(const K& key, const std::shared_ptr<Node>& node, search_descriptor& desc) 
	{
		desc.position = node->keys.size();

		long int lo=0;
		long int hi=desc.position-1;	

		while (lo <= hi)
		{
			size_t mid = (hi - lo)/2 + lo;	
			if (node->keys.at(mid) > key) 
			{
				desc.position = mid;
				hi = mid - 1;
			}
			else if (node->keys.at(mid) < key)
			{
				lo = mid + 1;
			} else {
				desc.position = mid;
				desc.exact_match = true;
				return;
			}
		}
		desc.exact_match = false;
	}

	/* BINARY SEARCH
	 *
	 * this is correct because if i never find a key that's bigger than the target
	 * key then the position remains the last index because there are no keys bigger
	 * than the target key so if i insert i must insert to the right most position.
	 *
	 * if there are some keys that are bigger then their index becomes the insertion
	 * position because when i insert at their index everything to the right of that
	 * index shift and I insert in the correct position
	 *
	 * i = 0, j = size(keys)-1, desc.position = size(keys)
	 * while i <= j:
	 * 	mid = (i+j)/2:
	 * 		if current.keys[mid] > key:
	 * 			desc.position = mid
	 * 			j = mid - 1
	 * 		else if current.keys[mid] < key
	 * 			i = mid + 1
	 * 		else // equals
	 * 			desc.exact_match = true
	 * 			desc.position = mid
	 * 			return
	 * desc.exact_match = false
	 * */

	/*
	 * while c is not a leaf
	 * do binary search and approach from the right:
	 * 	the default value of biary search is out of bound on the keys array but
	 * 	in bound on the pointers array (this because it's a non-leaf node)
	 * 	if all the values are bigger then the minimum value approaching from
	 * 	the right can be is 0.
	 *	if == key.size() -> all values are smaller than key so go to the right most
	 *		pointer.
	 *	if key == key at (i) then go to the pointer that contains it
	 *	if key < key at (i) then go to the pointer that contains values less than it
	 *	remember K(i) doesn't belong in P(i) it belongs in P(i+1)
	 *
	 *	#################### bread crumbs #######################
	 *	i could be 0, keys.size(), or in bteween
	 *	if i is 0 
	 *		 if key at i equals target key then i have to go to p(1)
	 *		 	so p(0) is predecessor and k(0) is split key
	 *		 if key at i doesn't equal target key then i have to go to p(0)
	 *		 	so p(1) is postdecessor and split key is k(0)
	 *	if i is bigger than 0:
	 *		if key at i equals target key then i have to go to p(i+1)
	 *			this is valid even for i = keys.size() because pointers are
	 *				keys.size()+1 long
	 *			so p(i) is a predecessor and k(i) is the split key
	 *		if key at i less than target key then i have to go to p(i)
	 *			so p(i-1) is a predecessor and k(i) is the split key
	 * */
	//////////////////////////////////////////////
	/// Root data member.
	std::shared_ptr<Node> root;

	Btree(): root {std::make_shared<Node>()}
	{
		// keys is not a variant so it's already initialized by the constructor.
		root->keys.reserve(N-1);
		root->children = record_list();
		std::get<record_list>(root->children).reserve(N-1);
	}

	void find(K key, search_descriptor& desc)
	{
		

		std::shared_ptr<Node> current = root;

		/*
		std::cout << "key= " << key << "\n";
		std::cout << "root key size: " << root->keys.size() << "\n";
		if (root->is_leaf()) {
			std::cout << "root records size: " << std::get<record_list>(current->children).size() << "\n";
		} else {
			std::cout << "root pointers size: " << std::get<pointer_list>(current->children).size() << "\n";
		}
		*/

		while (not current->is_leaf()) {
			binary_search(key, current, desc);
			if (desc.position == 0) {
				if (key == current->keys.at(0))
				{
					if (desc.collect_breadcrumbs)
					{
					desc.bread_crumbs.insert(
						std::make_pair(
							std::get<pointer_list>(current->children).at(1),
							std::make_pair(1, current)));
					}
					current = std::get<pointer_list>(current->children).at(1);
				}	else {
					if (desc.collect_breadcrumbs)
					{
					desc.bread_crumbs.insert(
						std::make_pair(
							std::get<pointer_list>(current->children).at(0),
							std::make_pair(0, current)));
					}
					current = std::get<pointer_list>(current->children).at(0);
				}
			} else {
				if (desc.position < current->keys.size() and key == current->keys.at(desc.position))
				{
					if (desc.collect_breadcrumbs)
					{
					desc.bread_crumbs.insert(
						std::make_pair(
							std::get<pointer_list>(current->children).at(desc.position+1),
							std::make_pair(desc.position+1, current)));
					}
					current = std::get<pointer_list>(current->children).at(desc.position+1);
				}	else {
					if (desc.collect_breadcrumbs)
					{
					desc.bread_crumbs.insert(
						std::make_pair(
							std::get<pointer_list>(current->children).at(desc.position),
							std::make_pair(desc.position, current)));
					}
					current = std::get<pointer_list>(current->children).at(desc.position);
				}			
			}
		}
		binary_search(key, current, desc);
		desc.leaf = current;
	}

	void insert(K key, T record)
	{
		/* The tree is never empty because the constructor allways gives a leaf node.
		 * search_desctipor desc;
		 * desc.exact_match = False;
		 * desc.collect_breadcrumbs = True;
		 * find(key, desc);
		 * 
		 * # handle the leaf case before looping for the non-leaf
		 *
		 * desc.leaf->keys.insert(k, v);
		 *
		 * if (desc.leaf->size() >= N) {
		 *  K key_to_insert_in_parent;
		 *  std::shared_ptr<Node> pointer_to_insert_in_parent;
		 * }
		 * */

		// configuring search options.
		search_descriptor desc;
		desc.exact_match = true;
		desc.collect_breadcrumbs = true;

		// Search.
		find(key, desc);

		std::shared_ptr<Node> current = desc.leaf;

		// Inserting element.
		current->keys.insert(current->keys.begin()+desc.position, key);
		std::get<record_list>(current->children)
			.insert(
				std::get<record_list>(current->children)
					.begin()+desc.position, record);

		// Check if the node has more than N-1 keys 
		if (current->keys.size() == N) {
			// create new node
			std::shared_ptr<Node> new_node = std::make_shared<Node>();
			new_node->children = std::vector<T>();
			new_node->keys.reserve(N-1);
			std::get<record_list>(new_node->children).reserve(N-1);

			// Swap next pointer.
			new_node->next = current->next;// ??? should lock before assign?? maybe
			current->next = new_node;

			// Copy keys from node that overflowed into new node.
			new_node->keys.insert(
				new_node->keys.begin(),
				current->keys.begin()+(N/2),
				current->keys.end());

			// Copy records from node that overflowed into new node.
			std::get<record_list>(new_node->children).insert(
				std::get<record_list>(new_node->children).begin(),
				std::get<record_list>(desc.leaf->children).begin()+(N/2),
				std::get<record_list>(desc.leaf->children).end());

			// Erase copied keys.
			current->keys.erase(
				current->keys.begin()+N/2,
				current->keys.end());

			// Erase copied records.
			std::get<record_list>(current->children).erase(
				std::get<record_list>(current->children).begin()+N/2,
				std::get<record_list>(current->children).end());

			// std::cout << *(desc.leaf) << "\n";
			// std::cout << *(new_node) << "\n";

			K new_key = new_node->keys.at(0);

			while (true){

				if (root == current) {
					std::shared_ptr<Node> new_root = std::make_shared<Node>();
					new_root->keys.reserve(N-1);
					new_root->children = pointer_list();
					std::get<pointer_list>(new_root->children).reserve(N);

					new_root->keys.push_back(new_key);
					std::get<pointer_list>(new_root->children).push_back(current);
					std::get<pointer_list>(new_root->children).push_back(new_node);

					root = new_root;
					return;
				}

				auto bread_crumb = desc.bread_crumbs.find(current); 

				if (bread_crumb == desc.bread_crumbs.end()) 
					throw std::runtime_error("ERROR: A non-root node must have a parent!");

				size_t position; 

				std::tie(position, current) = bread_crumb->second;

				current->keys.insert(current->keys.begin()+position, new_key);
				std::get<pointer_list>(current->children)
					.insert(std::get<pointer_list>(current->children).begin()+position+1, new_node);
	

				if (current->keys.size() <= N-1) break;

				new_node = std::make_shared<Node>();
				new_node->keys.reserve(N-1);
				std::get<pointer_list>(new_node->children).reserve(N);

				new_key = current->keys.at(std::ceil(static_cast<double>(N)/2));

				new_node->keys.insert(new_node->keys.begin(),
													current->keys.begin() + std::ceil(static_cast<double>(N)/2)+1,
													current->keys.end());

				current->keys.erase(
													current->keys.begin() + std::ceil(static_cast<double>(N)/2),
													current->keys.end());
				
				std::get<pointer_list>( new_node->children )
					.insert(std::get<pointer_list>( new_node->children ).begin(),
						 		  std::get<pointer_list>( current->children ).begin() + std::ceil(static_cast<double>(N+1)/2),
						 			std::get<pointer_list>( current->children ).end());

				std::get<pointer_list>( current->children )
					.erase(std::get<pointer_list>( current->children ).begin() + std::ceil(static_cast<double>(N+1)/2),
						 		 std::get<pointer_list>( current->children ).end());
			}
		}
	}

	void remove(search_descriptor& desc) 
	{
		std::shared_ptr<Node> current = desc.leaf;
		size_t position = desc.position;
		key_list& keys = current->keys;
		record_list& records = std::get<record_list>(current->children);

		// Delete entry.
		keys.erase(keys.begin()+position);
		records.erase(records.begin()+position);

		if (root == desc.leaf or keys.size() >= N/2) return;

		auto bread_crumb = desc.bread_crumbs.find(desc.leaf);

		if (bread_crumb == desc.bread_crumbs.end()) 
			throw std::runtime_error("ERROR: non root node must have a parent.");

		size_t inparent_position;
		std::shared_ptr<Node> parent;

		std::tie(inparent_position, parent) = bread_crumb->second;

		if (inparent_position == 0) {
			std::shared_ptr<Node> neighbour = std::get<pointer_list>(parent->children).at(1);
			key_list& neighbour_keys = neighbour->keys;
			record_list& neighbour_records = std::get<record_list>(neighbour->children);
			
			if (keys.size() + neighbour_keys.size() < N) {
				// Append neighbour to current.
				keys.insert(keys.end(), neighbour_keys.begin(), neighbour_keys.end());
				records.insert(records.end(), neighbour_records.begin(), neighbour_records.end());	
				// Swap next pointer.
				current->next = neighbour->next;
				// Delete from parent.
				remove_from_parent(1, parent, desc);
			} else {
				keys.push_back(neighbour_keys.begin());
				records.push_back(neighbour_records.begin());

				neighbour_keys.erase(neighbour_keys.begin());
				neighbour_records.erase(neighbour_records.begin());

				parent->keys[inparent_position]=neighbour_keys.front();
			}
		} else {
			std::shared_ptr<Node> neighbour = std::get<pointer_list>(parent->children).at(position-1);
			key_list& neighbour_keys = neighbour->keys;
			record_list& neighbour_records = std::get<record_list>(neighbour->children);
			if (keys.size() + neighbour_keys.size() < N) {
				// Append current to neighbour
				neighbour_keys.insert(neighbour_keys.begin(), keys.begin(), keys.end());
				neighbour_records.insert(neighbour_records.begin(), records.begin(), records.end());
				neighbour->next = current->next;
				// Delete from parent.
				remove_from_parent(inparent_position, parent, desc);
			} else {
				keys.insert(keys.begin(), neighbour_keys.back());
				records.insert(records.begin(), neighbour_records.back());

				neighbour_keys.erase(neighbour_keys.end()-1);
				neighbour_records.erase(neighbour_records.end()-1);

				parent->keys[inparent_position-1] = keys.front();
			}
		}
	}

	void remove_from_parent(size_t position, std::shared_ptr<Node> current, search_descriptor& desc)
	{
		while (true) {
			key_list& keys = current->keys;
			pointer_list& pointers = std::get<pointer_list>(current->children);

			keys.erase(keys.begin()+position-1);
			pointers.erase(pointers.begin()+position);

			/*
			 * current could be the root return
			 * if the root and no keys make the last pointer new root
			 * */

			if (current == root) {
				if(keys.size() == 0) {
					root = pointers.at(0);
				}
				return;
			}

			if (keys.size >= N/2) return;

			/*
			 * Get neighbour
			 * */
			auto bread_crumb = desc.bread_crumbs.find(current);

			if (bread_crumb == desc.bread_crumbs.end()) 
				throw std::runtime_error("ERROR: non root node must have a parent.");

			size_t inparent_position;
			std::shared_ptr<Node> parent;

			std::tie(inparent_position, parent) = bread_crumb->second;

			if (inparent_position == 0) {
				K pivot = parent->keys.at(inparent_position);
				std::shared_ptr<Node> neighbour = std::get<pointer_list>(parent->children).at(1);	
				key_list& neighbour_keys = neighbour->keys;
				pointer_list& neighbour_pointers = std::get<pointer_list>(neighbour->children);

				if (keys.size() + neighbour_keys.size() < N) {
					keys.insert(keys.end(), neighbour_keys.begin(), neighbour_keys.end());
					pointers.insert(pointers.end(), neighbour_pointers.begin(), neighbour_pointers.end());
					current = parent;
					position = inparent_position;
				} else {
					keys.push_back(pivot);
					pointers.push_back(neighbour_pointers.front());
					parent->keys[inparent_position] = neighbour_keys.front();
					neighbour_keys.erase(neighbour_keys.begin());
					neighbour_pointers.erase(neighbour_pointers.begin());
					return;
				}
			} else {
				K pivot = parent->keys.at(inparent_position-1);
				std::shared_ptr<Node> neighbour = 
					std::get<pointer_list>(parent->children).at(inparent_position-1);	
				key_list& neighbour_keys = neighbour->keys;
				pointer_list& neighbour_pointers = std::get<pointer_list>(neighbour->children);

				if (keys.size() + neighbour_keys.size() < N) {
					neighbour_keys.insert(neighbour_keys.end(), keys.begin(), keys.end());
					neighbour_pointers.insert(neighbour_pointers.end(), pointers.begin(), pointers.end());
					current = parent;
					position = inparent_position-1;
				} else {
					keys.insert(keys.begin(), pivot);
					pointers.insert(pointers.begin(), neighbour_pointers.back());
					parent->keys[inparent_position-1] = neighbour_keys.back();
					neighbour_keys.pop_back();
					neighbour_pointers.pop_back();
					return;
				}
			}
		}
	}

};

}

#endif
