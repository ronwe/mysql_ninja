// https://zhuanlan.zhihu.com/p/36526740

class LruCache {
    constructor(limit) {
        this.limit = limit || 10
        //head 指针指向表头元素，即为最常用的元素
        this.head = this.tail = undefined
        this.map = {}
        this.size = 0
		this.onDel = null
    }
    get(key, IfreturnNode) {
        let node = this.map[key]
        if (node === undefined) return
        if (node === this.head) { //判断该节点是不是是第一个节点
            return IfreturnNode ?
                node :
                node.value
        }
        // 不是头结点，铁定要移动元素了
        if (node.prev) { //首先要判断该节点是不是有前驱
            if (node === this.tail) { //有前驱，若是尾节点的话多一步，让尾指针指向当前节点的前驱
                this.tail = node.prev
            }
            node.prev.next = node.next
        }
        if (node.next) { //判断该节点是不是有后继
            //有后继的话直接让后继的前驱指向当前节点的前驱
            node.next.prev = node.prev
            //整个一个过程就是把当前节点拿出来，并且保证链表不断，下面开始移动当前节点了
        }
        node.prev = undefined //移动到最前面，所以没了前驱
        node.next = this.head //注意！！！ 这里要先把之前的排头给接到手！！！！让当前节点的后继指向原排头
        if (this.head) {
            this.head.prev = node //让之前的排头的前驱指向现在的节点
        }
        this.head = node //完成了交接，才能执行此步！不然就找不到之前的排头啦！
        return IfreturnNode ?
            node :
            node.value
    }
    set(key, value) {
        let node = this.get(key, true)
        if (!node) {
            if (this.size === this.limit) { //判断缓存是否达到上限
				if (this.tail){
					if (this.onDel) {
						this.onDel(this.tail.key)
					}
                    delete this.map[this.tail.key] 
                    this.tail.prev.next = undefined
                    this.tail = this.tail.prev
                    this.size--
				}
            }
			node = {
				key: key
			}
			this.map[key] = node
			if(this.head){//判断缓存里面是不是有节点
				this.head.prev = node
				node.next = this.head
				node.pre = undefined

				this.head = node
			}else{
				//缓存里没有值，皆大欢喜，直接让head指向新节点就行了
				this.head = node
				this.tail = node
			}

			this.size++//减少一个缓存槽位
        }

        node.value = value
    }

	del(key) {
        let node = this.map[key]
		if (node){
			let pre = node.prev
				,next = node.next
			if (this.tail === node){
				this.tail = pre 
				this.tail.next = undefined
			}else if (this.head === node){
				this.head = next
				this.head.pre = undefined
			}else if (pre && next) {
				pre.next = next
				next.pre = pre
			}	
			if (this.onDel) {
				this.onDel(node.key)
			}

            delete this.map[key] 
			this.size--
		}
	}

	onDel(handler) {
		this.onDel = handler
	}
}

module.exports = LruCache
