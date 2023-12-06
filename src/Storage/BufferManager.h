


#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include <any>
#include<list>
#include<mutex>
#include<shared_mutex>
#include <type_traits>
#include<unordered_map>
#include<cstring>
#include<string>
#include<fstream>
#include<algorithm>
#include<set>
#include <vector>
#include <functional>
#include <map>



#define DISALLOW_COPY(cname)                                    \
  cname(const cname &) = delete;                   /* NOLINT */ \
  auto operator=(const cname &)->cname & = delete; /* NOLINT */

#define DISALLOW_MOVE(cname)                               \
  cname(cname &&) = delete;                   /* NOLINT */ \
  auto operator=(cname &&)->cname & = delete; /* NOLINT */

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);


#define EXTERN_FILE_MANAGER_FUNCTION_TEMPLATE_PARAMETER(x) \
    template class FileHandle<x>;                           \
    template class Page<x>;                                 \
    template std::tuple<FileHandle<x>,bool> FileManager::open<x>(const std::string&,std::ios_base::openmode,int); \
    template Page<x>* FileManager::GetPage<x>(const std::string&,page_id_t);         \
    template Page<x>* FileManager::GetNewPage<x>(const std::string&,page_id_t*);      \
    template void FileManager::UnPin<x>(const std::string&,page_id_t ,bool);           \
    template void  FileManager::ReadPage<x>(std::fstream&,const std::string&,page_id_t,char*);         \
    template void  FileManager::WritePage<x>(std::fstream&,const std::string&,page_id_t,const char*);        \
    template void  FileManager::FlushPage<x>(std::fstream&,const std::string&,page_id_t,std::unordered_map<page_id_t,frame_id_t>&);        \
    //template void  FileManager:FlushOneFileAllPages<x>(const std::string&);              \
    template void  FileManager::FlushAllFile<x>();     \

#define EXTERN_FILE_MANAGER_OPEN_TEMPLATE_PARAMETER(PAGE_SIZE,PAGE_NUM) 











static constexpr int BUCKET_PAGE_SIZE = 4096;

/*
--------------------------------------------------------------
SCN|PAGE_ID|
--------------------------------------------------------------

*/

using page_id_t = uint32_t ;
using frame_id_t = uint32_t;

template<uint32_t PAGE_SIZE>
class Page;
template<uint32_t PAGE_SIZE>
class FileHandle;
class FileManager;

template<class K,class V>
class BucketPage;
class DirectoryPage;
class LRUKReplacer;


/*
------------------------------------------------------
SCN|
------------------------------------------------------

*/
template<uint32_t PAGE_SIZE>
class Page{
private:
    static constexpr uint32_t _PAGE_SIZE  = PAGE_SIZE;
    std::shared_mutex rw_lock_;
public:
    int page_id=-1;
    int pin_=0;

    bool is_dirty=false;
    char data[PAGE_SIZE];
public:


    Page();
    ~Page();
    Page(const Page& other);
    Page(Page&& other);
    inline char* GetData(){return data;}
    void ReadLock();
    void ReadUnlock();
    void WriteLock();
    void WriteUnlock();
    uint32_t GetPageSize(){
        return _PAGE_SIZE;
    }

    void ResetMemory();
};


/**************************/
/*
OFFSET point to next position should be inserted.
----------------------------------------------
SCN|OFFSET|SIZE|LOCAL_DEPTH|
----------------------------------------------

*/
template<class K,class V>
class BucketPage:public Page<BUCKET_PAGE_SIZE>{
    
private:
    class Iterator{
        char* page_data_=nullptr;
        int offset_=-1;
        int size_=-1;
        int count_=0;
        K* k=nullptr;
        V* v=nullptr;
    public:
        Iterator();
        Iterator(char* page_data);
        ~Iterator();
        
        void operator++();
        bool operator==(const Iterator& other);
        bool operator!=(const Iterator& other);
        std::pair<K,V> operator*();
        inline int GetOffset(){return  offset_;}
        
    };

private:
    bool removeOnce(const K& key);

    bool removeOnce(const K& key,const V& value);
    inline void Move(int from,int to,int n){
        memcpy(data+to,data+from,n);
    }
public:
    static constexpr int HEADER_SIZE=16;
private:
    static constexpr int SCN=0;
    static constexpr int OFFSET_OFFSET=4;
    
    static constexpr int SIZE_OFFSET=8;
    static constexpr int LOCAL_DEPTH_OFFSET=12;

    
public:

    inline int GetSCN(){
        return *(uint32_t*)(data+SCN);
    }

    inline int GetOffset(){
        return *(uint32_t*)(data+OFFSET_OFFSET);
    }
    inline int GetSize(){
        return *(uint32_t*)(data+SIZE_OFFSET);
    }
    inline int GetLocalDepth(){
        return *(uint32_t*)(data+LOCAL_DEPTH_OFFSET);
    }
    
    inline void SetSCN(int scn){
        *(uint32_t*)(data+SCN)=scn;
    }

    inline void SetOffset(int offset){
        *(uint32_t*)(data+OFFSET_OFFSET)=offset;
    }
    inline void SetSize(int size){
        *(uint32_t*)(data+SIZE_OFFSET)=size;
    }
    inline void SetLocalDepth(int local_depth){
        *(uint32_t*)(data+LOCAL_DEPTH_OFFSET)=local_depth;
    }
    
    bool Insert(const K& key,const V& value);

    int remove(const K& key);
    int remove(const K& key,const V& value);

    int Find(const K& k,std::vector<std::pair<K,V>>* vec);


    Iterator Begin();

        
    Iterator End();

    
};


/*******************************/
/*
    The first page store meta data
    -----------------------------------------
    PAGE_NUM|GLOBAL_DEPTH|KEY_SIZE|VALUE_SIZE
    ------------------------------------------

*/
class DirectoryHeader:public Page<BUCKET_PAGE_SIZE>{
public:
    static constexpr int INITCIAL_GLOBAL_DEPTH=10;

private:
    static constexpr int PAGE_NUM_OFFSET=0;
    static constexpr int GLOBAL_DEPTH_OFFSET=4;
    static constexpr int KEY_SIZE_OFFSET=8;
    static constexpr int VALUE_SIZE_OFFSET=12;
public:
    inline int GetPageNum(){return *(uint32_t*)(data+PAGE_NUM_OFFSET);}
    inline int GetGlobalDepth(){
        return *(uint32_t*)(data+GLOBAL_DEPTH_OFFSET);
    }
    inline int GetKeySize(){
        return *(uint32_t*)(data+KEY_SIZE_OFFSET);
    }
    inline int GetValueSize(){
        return *(uint32_t*)(data+VALUE_SIZE_OFFSET);
    }

    inline void SetPageNum(int page_num){
        *(uint32_t*)(data+PAGE_NUM_OFFSET)=page_num;
    }
    inline void SetGlobalDepth(int global_depth){
        *(uint32_t*)(data+GLOBAL_DEPTH_OFFSET)=global_depth;
    }
    inline void SetKeySize(int key_size){
        *(uint32_t*)(data+KEY_SIZE_OFFSET)=key_size;
    }
    inline void SetValueSize(int value_size){
        *(uint32_t*)(data+VALUE_SIZE_OFFSET)=value_size;
    }


};

/**
 * 
 DirectoryPage has nothing except bucket addr(page_id)
 * 
 */
class DirectoryPage:public Page<BUCKET_PAGE_SIZE>{
public:
/**
 * @brief Set the Page Id object
 * 
 * @param h ,0-2047
 * @param page_id pageid
 */
    void SetBucketId(int h,int page_id){
        *(uint32_t*)(data+h*4)=page_id;
    }
    /**
     * @brief Get the Bucket Id object
     * 
     * @param h 0-2047
     * @param page_id 
     * @return int bucketId
     */
    int GetBucketId(int h){
        return *(uint32_t*)(data+h*4);
    }
};



class FileManager{
    template<uint32_t PAGE_SIZE>
    friend class FileHandle; 
private:

//every file has its own buffer_pool
    std::map<std::string,void *> buffer_pools_;

    // void (FileManager::*deleter)() = nullptr;

    std::unordered_map<std::string,std::function<void()>> deleters_;
    std::unordered_map<std::string,std::function<void()>> flushers_;

    std::unordered_map<std::string,std::unordered_map<page_id_t,frame_id_t>> maps_;
    std::unordered_map<std::string,std::fstream> files_;
    std::unordered_map<std::string,LRUKReplacer*> replacers_;
    std::unordered_map<std::string,std::list<int>>  free_lists_;

    std::unordered_map<std::string,int> page_counts_;

    std::set<std::string> files_name_;
    std::mutex lock_;
public:    
    FileManager();
    ~FileManager();
    DISALLOW_COPY_AND_MOVE(FileManager)
    template<uint32_t PAGE_SIZE>
    std::tuple<FileHandle<PAGE_SIZE>,bool> open(const  std::string& path, 
    std::ios_base::openmode flag,int page_nums);


    int static GetFileSize(std::fstream& path);

private:
    template<uint32_t PAGE_SIZE>
    Page<PAGE_SIZE>* GetPage(const std::string& path,page_id_t page_id);
    template<uint32_t PAGE_SIZE>
    Page<PAGE_SIZE>* GetNewPage(const std::string& path,page_id_t* page_id);
    
    template<uint32_t PAGE_SIZE>
    void UnPin(const std::string& path,page_id_t page_id,bool is_dirty);

    template<uint32_t PAGE_SIZE>    
    void ReadPage(std::fstream& f,
                const std::string& path,
                page_id_t page_id,
                char* page_data);

    template<uint32_t PAGE_SIZE>
    void WritePage(std::fstream& f,const std::string& path,
page_id_t page_id,const char* page_data);

    template<uint32_t PAGE_SIZE>
    void FlushPage(std::fstream& f,const std::string& path,
        page_id_t page_id,std::unordered_map<page_id_t,frame_id_t>& map);
    template<uint32_t PAGE_SIZE>
    void FlushOneFileAllPages(const std::string& path);
    
    void FlushAllFile();
};




template<uint32_t PAGE_SIZE>
class FileHandle{
private:
    std::string path_;
    FileManager* file_manager_=nullptr;
public:
    FileHandle();
    FileHandle(const std::string& path,FileManager& file_manager);
    FileHandle(const FileHandle& other);
    FileHandle& operator=(const FileHandle& other);
    ~FileHandle();
    
    uint32_t GetPageSize(){
        return PAGE_SIZE;
    }
    inline const std::string& GetFileName(){return path_;}
    inline int GetPageNum(){
        return file_manager_->page_counts_[path_];
    }

    Page<PAGE_SIZE>* GetPage(page_id_t page_id);
    
    Page<PAGE_SIZE>* GetNewPage(page_id_t* page_id);
    void Unpin(page_id_t page_id,bool is_dirty);
    
};








class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
    size_t current_timestamp_{0};
    size_t curr_size_{0};
    size_t replacer_size_;
    size_t max_size_;
    size_t k_;
    std::mutex latch_;

    using timestamp = std::list<size_t>; // use a list store timestamp

    using k_time = std::pair<frame_id_t, size_t>;
    std::unordered_map<frame_id_t,timestamp> hist_;
    std::unordered_map<frame_id_t, size_t> recorder_;
    std::unordered_map<frame_id_t, bool> evictable_;


    std::list<frame_id_t> new_frame_;
    std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> new_locate_;

    std::list<k_time> cache_frame_;
    std::unordered_map<frame_id_t, std::list<k_time>::iterator> cache_locate_;

    static auto CmpKtime(const k_time& f1,const k_time& f2)->bool{
        return f1.second < f2.second;
    }
    
};



#endif
