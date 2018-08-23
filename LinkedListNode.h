class LinkedListNode {
  public:
    int val;
    LinkedListNode *next;
    LinkedListNode(int);
};

LinkedListNode::LinkedListNode (int v) {
  val = v;
  next = 0;
}

class LinkedList {
  public:
    LinkedListNode *head;
    LinkedListNode *tail;
    void enqueue(int);
    void remove(int);
    int getMax();
    ~LinkedList();
};

LinkedList::~LinkedList() {
  LinkedListNode *current = head;
  while (current != 0) {
    LinkedListNode *next = current->next;
    delete current;
    current = next;
  }
}

// Returns -1 if no max found
int LinkedList::getMax() {
  LinkedListNode *current = head;
  int max = -1;
  while (current != 0) {
    if (current->val > max) {
      max = current->val;
    }
    current = current->next;
  }
  return max;
}

// O(1)
void LinkedList::enqueue(int val) {
  if (tail != 0) {
    tail->next = new LinkedListNode(val);
    tail = tail->next;
  } else {
    head = new LinkedListNode(val);
    tail = head;
  }
}

// Remove first occurrence
void LinkedList::remove(int val) {
  LinkedListNode *current = head;
  LinkedListNode *prev = 0;
  while (current != 0) {
    if (current->val == val) {
      if (head == current) {
        head = head->next;
      }
      if (tail == current) {
        tail = prev;
      }
      if (prev != 0) {
        prev->next = current->next;
      }
      current->next = 0;
      delete current;
      break;
    }
    prev = current;
    current = current->next;
  }
}
