#pragma once
namespace csi {
  template<class K>
  class counter_store
  {
  public:
    virtual ~counter_store() {}

    virtual void close() = 0;
    /**
    * Put a key-value pair
    */
    virtual void add(const K& key, size_t count) = 0;
    /**
    * Deletes a key-value pair with the given key
    */
    virtual void del(const K& key) = 0;
    /**
    * Returns a key-value pair with the given key
    */
    //virtual std::shared_ptr<krecord<K, size_t>> get(const K& key) = 0;
    virtual typename csi::kmaterialized_source<K, size_t>::iterator begin(void) = 0;
    virtual typename csi::kmaterialized_source<K, size_t>::iterator end() = 0;
  };
}; 