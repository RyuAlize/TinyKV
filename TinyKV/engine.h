
template<typename K, typename V>
class KVsEngine {
public:
	virtual ~KVsEngine(){}
	virtual void set(K* key, V* val) = 0;
	virtual V* get(K* key) = 0;
	virtual void remove(K* key) = 0;
};
