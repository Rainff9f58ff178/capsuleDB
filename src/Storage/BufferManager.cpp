

#include "BufferManager.h"
#include <climits>
#include <exception>

#include <ios>
#include <memory>
#include <sys/stat.h>
#include<condition_variable>
#include<mutex>
#include <cassert>
#include<format>
#include<span>
constexpr const int INCLUDE_ALL=INT_MAX;






EXTERN_FILE_MANAGER_FUNCTION_TEMPLATE_PARAMETER(1024);
EXTERN_FILE_MANAGER_FUNCTION_TEMPLATE_PARAMETER(8192);
EXTERN_FILE_MANAGER_FUNCTION_TEMPLATE_PARAMETER(4096);
EXTERN_FILE_MANAGER_FUNCTION_TEMPLATE_PARAMETER(256*1024);

#define PAGE_TEMPLATE template<uint32_t PAGE_SIZE>
PAGE_TEMPLATE
Page<PAGE_SIZE>::Page()  { ResetMemory(); }

PAGE_TEMPLATE
Page<PAGE_SIZE>::~Page() {}
PAGE_TEMPLATE
Page<PAGE_SIZE>::Page(Page&& page){
    page_id = page.page_id;
    pin_ = page.pin_;
    is_dirty = page.is_dirty;
    this->rw_lock_.lock_shared();
    memcpy(data,page.data,PAGE_SIZE);
}
PAGE_TEMPLATE
Page<PAGE_SIZE>::Page(const Page& page){
    page_id = page.page_id;
    pin_ = page.pin_;
    is_dirty = page.is_dirty;
    this->rw_lock_.lock_shared();
    memcpy(data,page.data,PAGE_SIZE);
    this->rw_lock_.unlock_shared();
    this->rw_lock_.unlock_shared();
}
PAGE_TEMPLATE
void
Page<PAGE_SIZE>::ReadLock() {
    rw_lock_.lock_shared();
}
PAGE_TEMPLATE
void
Page<PAGE_SIZE>::WriteLock() {
    rw_lock_.lock();
}

PAGE_TEMPLATE
void
Page<PAGE_SIZE>::ReadUnlock() {
    rw_lock_.unlock_shared();
}
PAGE_TEMPLATE
void
Page<PAGE_SIZE>::WriteUnlock() {
    rw_lock_.unlock();
}

PAGE_TEMPLATE
void
Page<PAGE_SIZE>::ResetMemory() {
    memset(data, 0, PAGE_SIZE);
}

/************************************************/
// BucketPage

/**
 * @brief you can insert multi key,but can't same key-value,if that return true;
 *
 * @tparam K
 * @tparam V
 * @param key
 * @param value
 * @return true
 * @return false
 */
template <class K, class V>
bool
BucketPage<K, V>::Insert(const K &key, const V &value) {
    std::vector<std::pair<K, V>> vec;
    auto i = Find(key, &vec);
    auto result = std::find_if(vec.begin(), vec.end(), [&](auto &it) {
        if (it.first == key && it.second == value) {
            return true;
        }
        return false;
    });

    if (result != vec.end()) {
        return true;
    }

    auto offset = GetOffset();
    if (offset + sizeof(K) + sizeof(V) > BUCKET_PAGE_SIZE) {

        return false;
    }

    *(K *) (data + offset) = key;
    offset += sizeof(K);
    *(V *) (data + offset) = value;
    offset += sizeof(V);
    SetSize(GetSize() + 1);
    SetOffset(offset);
    return true;
}

template <class K, class V>
int
BucketPage<K, V>::Find(const K &k, std::vector<std::pair<K, V>> *vec) {
    auto it = Begin();
    int num = 0;
    for (; it != End(); ++it) {
        auto [_k, v] = *it;
        if (_k == k) {
            if (vec != nullptr) {
                vec->push_back({std::move(_k), std::move(v)});
            }
            num++;
        }
    }
    return num;
}

template <class K, class V>
int
BucketPage<K, V>::remove(const K &key) {
    int num = 0;
    while (removeOnce(key)) {
        num++;
    }
    return num;
}

template <class K, class V>
int
BucketPage<K, V>::remove(const K &key, const V &value) {
    int num = 0;
    while (removeOnce(key, value)) {
        num++;
    }
    return num;
}

template <class K, class V>
bool
BucketPage<K, V>::removeOnce(const K &key) {
    auto it = Begin();
    for (; it != End(); ++it) {
        auto [k, v] = *it;
        if (k == key) {
            break;
        }
    }
    if (it == End()) {
        return false;
    }
    for (; it != End(); ++it) {
        auto offset = it.GetOffset();
        Move(offset, offset - sizeof(K) - sizeof(V), sizeof(K) + sizeof(V));
    }
    SetSize(GetSize() - 1);
    SetOffset(GetOffset() - sizeof(K) - sizeof(V));
    return true;
}
template <class K, class V>
bool
BucketPage<K, V>::removeOnce(const K &key, const V &value) {
    auto it = Begin();
    for (; it != End(); ++it) {
        auto [k, v] = *it;
        if (k == key && v == value) {
            break;
        }
    }
    if (it == End()) {
        return false;
    }
    for (; it != End(); ++it) {
        auto offset = it.GetOffset();
        Move(offset, offset - sizeof(K) - sizeof(V), sizeof(K) + sizeof(V));
    }
    SetSize(GetSize() - 1);
    SetOffset(GetOffset() - sizeof(K) - sizeof(V));
    return true;
}

template <class K, class V>
auto
BucketPage<K, V>::Begin() -> BucketPage::Iterator {
    return Iterator(data);
}

template <class K, class V>
auto
BucketPage<K, V>::End() -> BucketPage::Iterator {
    return Iterator();
}

template <class K, class V> BucketPage<K, V>::Iterator::Iterator() {}

template <class K, class V> BucketPage<K, V>::Iterator::~Iterator() {}

template <class K, class V>
BucketPage<K, V>::Iterator::Iterator(char *page_data) : page_data_(page_data) {
    offset_ = BucketPage::HEADER_SIZE;
    size_ = *(uint32_t *) (page_data + BucketPage::SIZE_OFFSET);
    operator++();
}

template <class K, class V>
auto
BucketPage<K, V>::Iterator::operator++() -> void {
    if (count_ < size_) {
        k = (K *) (page_data_ + offset_);
        assert(k<(K*)page_data_+BUCKET_PAGE_SIZE);
        offset_ += sizeof(K);
        v = (V *) (page_data_ + offset_);
        assert(v<(V*)page_data_+BUCKET_PAGE_SIZE);
        offset_ += sizeof(V);
        count_++;
        return;
    }
    page_data_ = nullptr;
    offset_ = -1;
    size_ = -1;
    count_ = 0;
    k = nullptr;
    v = nullptr;
}

template <class K, class V>
bool
BucketPage<K, V>::Iterator::operator==(const Iterator &other) {
    if (page_data_ == other.page_data_ && offset_ == other.offset_ &&
        size_ == other.size_ && count_ == other.count_ && k == other.k &&
        v == other.v) {
        return true;
    }
    return false;
}

template <class K, class V>
bool
BucketPage<K, V>::Iterator::operator!=(const Iterator &other) {
    return !(*this == other);
}

template <class K, class V>
std::pair<K, V>
BucketPage<K, V>::Iterator::operator*() {
    return {*k, *v};
}

template class BucketPage<int32_t, int32_t>;
template class BucketPage<uint64_t, uint64_t>;
template class FileHandle<256>;



template<uint32_t PAGE_SIZE>
DiskManager<PAGE_SIZE>::DiskManager(const std::string& path,
std::ios_base::openmode flag,int page_nums,bool& is_first_load)
{
        std::fstream f;
        f.open(path,flag | std::ios::ate);
        if(!f.is_open()){
            f.open(path,flag | std::ios::trunc | std::ios::ate);
        }
        bool first_loaded = true;
        auto size = GetFileSize(f);
        if(size > 0){
            first_loaded = false;
        }
        if(size % PAGE_SIZE != 0 && !first_loaded)
            throw std::runtime_error(std::format("file size % PAGE_SIZE is not zero, file size is {},page size is {} ",size,PAGE_SIZE));
        
        f_ = std::move(f);

        for(uint32_t i =0 ; i<page_nums;++i){
            pages_.push_back(new Page<PAGE_SIZE>());
        }

        for(uint32_t i=0;i<page_nums;++i){
            free_lists_.push_back(i);
        }

        replacer_ = std::make_shared<LRUKReplacer>(page_nums,2);
        if(first_loaded){
            // use first page for allocate page.
            page_counts_ = 0;
            page_id_t _p_id;
            auto* page = GetNewPage(&_p_id);
            page->is_load_ = true;
            assert(page->page_id==0);
            // use for bit map;

            Unpin(page->page_id,false);
        }else {
            page_counts_ = size /PAGE_SIZE;
        }
        is_first_load = first_loaded;

        // enable background flush thread.
        logger_ = std::make_shared<Logger>(path+".log");

        logger_->AppendDEBUG("log begin");
        
        backgroud_flush_thread_ = std::thread([this](){
            // flush page that 
            std::mutex mtx;
            std::condition_variable cv_;
            std::unique_lock<std::mutex> lock(mtx);
            while(1){
                for(uint32_t i=0;i<pages_.size();++i){
                    auto* page = pages_[i];
                    if(page->page_id == -1)
                        continue;
                //can i flush a page pin_ is not 0 ? no , a page may be isn't write finished even throughj it doesn't accuire lock;
                    if(page->tryWriteLock() ){
                        if( !(page->pin_ == 0 && page->is_dirty)){
                            page->WriteUnlock();
                            continue;
                        }
                        FlushPage(page->page_id);   
                        logger_->AppendDEBUG(std::format("flush down page {}",page->page_id));
                        page->is_dirty = false;

                        page->WriteUnlock();
                    }

                }
                if(finished_)
                    break;
                cv_.wait_for(lock,std::chrono::milliseconds(100));
            }

            
        });
}




template<uint32_t PAGE_SIZE>
Page<PAGE_SIZE>* DiskManager<PAGE_SIZE>::GetNewPage(page_id_t* page_id){
    std::unique_lock<std::mutex> l(mtx_);
    auto new_page_id = page_counts_++;
    l.unlock();

    std::array<char,PAGE_SIZE> data{0};
    WritePage(new_page_id,data.data());
    *page_id = new_page_id;
    return GetPage(new_page_id);
}



template<uint32_t PAGE_SIZE>
Page<PAGE_SIZE>* DiskManager<PAGE_SIZE>::GetPage(page_id_t page_id){
    std::unique_lock<std::mutex> lock(mtx_);
    if(page_counts_ <= page_id){
        throw std::runtime_error("You try to get a page that not allocated");
    }

    // check if this page already exist in buffer pools.

    auto it =
        std::find_if(std::begin(maps_), std::end(maps_), [&](auto &it) -> bool {
            if (it.first == page_id) {
                return true;
            }
            return false;
        });
    

    if(it != std::end(maps_)){
        // alraedy exist in buffer pools.
        auto* page = pages_[maps_[page_id]];
        if(!page->is_load_){
            throw std::runtime_error("get page in memory but it doesnt load ?");
        }
        page->pin_++;
        replacer_->RecordAccess(maps_[page_id]);
        replacer_->SetEvictable(maps_[page_id],false);
        return  page;
    }
    // not in buffer pools.

    if(!free_lists_.empty()){
        // just allocate from free_lists_;
        auto idx = free_lists_.front();
        free_lists_.pop_front();
        auto* page = pages_[idx];
        assert(page->pin_==0);
        page->pin_=1;
        page->page_id = page_id;
        page->is_dirty = false;
        replacer_->RecordAccess(idx);
        maps_.insert({page_id,idx});
        replacer_->SetEvictable(idx,false);
        return page;
    }

    // there are no free list in memory.
    frame_id_t victim_page_idx;
    if(!replacer_->Evict(&victim_page_idx)){
        throw std::runtime_error("Evict Failed,Mayby all pages in memory is UnEvictable");
    }

    // evict successfully.
    Page<PAGE_SIZE>* vic_page = pages_[victim_page_idx];
    //Evictable page 's pin must 0.
    assert(vic_page->pin_==0);
    assert(vic_page->is_load_ == true); // this page must alraedy loaded.
    if(vic_page->is_dirty){
        FlushPage(vic_page->page_id);
    }
    maps_.erase(vic_page->page_id); // buffer poll no more storo this page.
    vic_page->is_dirty = false;
    vic_page->pin_ =1;
    vic_page->page_id = page_id;
    vic_page->is_load_ = false;
    maps_.insert({page_id,victim_page_idx}); // new map.
    replacer_->RecordAccess(victim_page_idx);
    replacer_->SetEvictable(victim_page_idx,false);
    return  vic_page;
}


template<uint32_t PAGE_SIZE>
void DiskManager<PAGE_SIZE>::Unpin(page_id_t page_id,bool is_dirty){
    
    std::unique_lock<std::mutex> l(mtx_);
    // this page must in memory.

    auto it = std::find_if(std::begin(maps_), std::end(maps_), [&](auto &it) {
        if (it.first == page_id) {
            return true;
        }
        return false;
    });

    if(it == std::end(maps_))
        throw std::runtime_error("You Unpin a page that not in memory ?");

    auto* page = pages_[it->second];
    if(!page->is_load_)
        throw std::runtime_error("You Unpin a page that not loaded (is_loaded_) ?");
    
    if(page->pin_ ==0)
        throw std::runtime_error("Your Unpin a page that pin_ == 0 ?");
    page->WriteLock();
    page->pin_--;

    if(page->pin_ == 0)
        replacer_->SetEvictable(it->second,true); 
    page->is_dirty = page->is_dirty | is_dirty;

    page->WriteUnlock();
}

template<uint32_t PAGE_SIZE>
void DiskManager<PAGE_SIZE>::ReadPage(page_id_t page_id,char* page_data){
    uint32_t offset = page_id * PAGE_SIZE;

    if(offset > GetFileSize(f_)){
        std::runtime_error("read past of the file ?");
    }
    std::unique_lock<std::mutex> lock(mtx_for_fstream_);
    f_.seekp(offset);
    f_.read(page_data,PAGE_SIZE);
    if(f_.bad())
        throw std::runtime_error("Io error when ReadPage");

    auto read_count = f_.gcount();
    lock.unlock();
    if(read_count<PAGE_SIZE){
        throw std::runtime_error("FATAL:Read byte less that one page ?");
    }

}
template<uint32_t PAGE_SIZE>
void DiskManager<PAGE_SIZE>::WritePage(page_id_t page_id,const char* page_data){
    std::unique_lock<std::mutex> l(mtx_for_fstream_);
    uint32_t offset = page_id * PAGE_SIZE;
    f_.seekp(offset);
    f_.write(page_data,PAGE_SIZE);
    if(f_.bad()){
        throw std::runtime_error("fstream error when write a page");
    }
    f_.flush();
}

template<uint32_t PAGE_SIZE>
void DiskManager<PAGE_SIZE>::FlushPage(page_id_t page_id){
    // this page must in memory.
    auto it = std::find_if(std::begin(maps_), std::end(maps_), [&](auto &it) {
        if (it.first == page_id) {
            return true;
        }
        return false;
    });
    assert(it!= std::end(maps_));
    auto* page = pages_[it->second];
    assert(page->page_id == page_id);
    WritePage(page_id,page->data);
    
}



EXTERN_FILE_MANAGER_OPEN_TEMPLATE_PARAMETER(1024, 10);

/************************************************/
// FileHandle
template<uint32_t PAGE_SIZE>
FileHandle<PAGE_SIZE>::FileHandle() : file_manager_() {}
template<uint32_t PAGE_SIZE>
FileHandle<PAGE_SIZE>::FileHandle(const std::string &path, FileManager &file_manager,const LoadPageFunction& load_f)
    : file_manager_(&file_manager), path_(path),load_f_(load_f) {}
template<uint32_t PAGE_SIZE>
FileHandle<PAGE_SIZE>::FileHandle(const FileHandle &other) {
    path_ = other.path_;
    file_manager_ = other.file_manager_;
    load_f_= other.load_f_;
}
template<uint32_t PAGE_SIZE>
FileHandle<PAGE_SIZE>&
FileHandle<PAGE_SIZE>::operator=(const FileHandle &other) {
    path_ = other.path_;
    file_manager_ = other.file_manager_;
    load_f_ = other.load_f_;
    return *this;
}

template<uint32_t PAGE_SIZE>
Page<PAGE_SIZE> *
FileHandle<PAGE_SIZE>::GetNewPage(page_id_t *page_id) {
    auto* page=  file_manager_->GetNewPage<PAGE_SIZE>(path_, page_id);
    page->WriteLock();
    if(page->is_load_){
        throw  std::runtime_error("get a new page from buffermanager ,but it's already loaded?");
    }
    load_f_(page->page_id,page->data);
    page->is_load_ = true;
    page->WriteUnlock();
    return  page;
}
template<uint32_t PAGE_SIZE>
FileHandle<PAGE_SIZE>::~FileHandle() {}

template<uint32_t PAGE_SIZE>
Page<PAGE_SIZE> *
FileHandle<PAGE_SIZE>::GetPage(page_id_t page_id) {
    auto* page =  file_manager_->GetPage<PAGE_SIZE>(path_, page_id);
    page->WriteLock();
    if(!page->is_load_){
        load_f_(page->page_id,page->data);
        page->is_load_ = true;
    }
    page->WriteUnlock();
    return  page;
}

template<uint32_t PAGE_SIZE>
void
FileHandle<PAGE_SIZE>::Unpin(page_id_t page_id, bool is_dirty) {
    file_manager_->UnPin<PAGE_SIZE>(path_, page_id, is_dirty);
}

/*************************************************/
// FileManager
FileManager::FileManager() {}

template<uint32_t PAGE_SIZE>
std::tuple<FileHandle<PAGE_SIZE>,bool>
FileManager::open(const std::string &path, 
std::ios_base::openmode flag,
int page_nums) {

    std::unique_lock<std::mutex> l(lock_);
    auto it = std::find(std::begin(files_name_),std::end(files_name_),path);
    if(it != std::end(files_name_)){
        throw std::runtime_error("this file already opened");
    }
    bool is_first_load;
    disk_managers_[path] = new DiskManager<PAGE_SIZE>(path,flag,page_nums,is_first_load);
    
    auto delete_function = [path,this](){
        auto*  disk_manager =
             std::any_cast<DiskManager<PAGE_SIZE>*>(disk_managers_[path]);
        assert(disk_manager->GetPage() == PAGE_SIZE);
        delete disk_manager;
    };

    deleters_.insert({path,delete_function});

    // std::fstream f;
    // f.open(path, flag | std::ios::ate);

    // if (!f.is_open()) {
    //     f.open(path, flag | std::ios::trunc | std::ios::ate);
    // }
    // bool first_loaded = true;
    // auto size = GetFileSize(f);

    // if (size > 0) {
    //     first_loaded = false;
    // }

    // files_[path] = std::move(f);
    // auto* pages = new Page<PAGE_SIZE>[page_nums];
    // auto* pages = new std::array<Page<PAGE_SIZE>,PAGE_NUMS>;
    // auto* pages = new char[sizeof(Page<PAGE_SIZE>)*page_nums];
    // auto* pages = new Page<PAGE_SIZE>[page_nums]; // cause ASSERT error

    // buffer_pools_.insert({path, (void*)pages});
    // auto& lock = locks_[path];
    // // std::array<Page<PAGE_SIZE>,PAGE_NUM> array;
    // // std::vector<Page<PAGE_SIZE>> pages;
    // // int* i= new int[1024];
    
    // auto function_deleter = [=,this](){
    //     auto* pages = reinterpret_cast<Page<PAGE_SIZE>*>(buffer_pools_[path]);
    //     delete[] pages;
    // };
    // auto function_flusher = [=,this](){
    //     logger_.AppendDEBUG("Flushing Page ,Path :" + path);
    //     std::unique_lock<std::mutex>( this->locks_[path] );
    //     auto &f = files_[path];
    //     auto &table_map = maps_[path];
    //     auto* pages =reinterpret_cast<Page<PAGE_SIZE>*>((buffer_pools_[path]));
    //     for (auto &it : table_map) {
    //         auto idx = it.second;
    //         Page<PAGE_SIZE>* page = &pages [idx];
    //         if (page->is_dirty) {
    //             FlushPage<PAGE_SIZE>(f, path, page->page_id, table_map);
    //             logger_.AppendDEBUG(std::format("flushed page_id {}",page->page_id));
    //         }
    //     }
    // };

    // // deleters_.insert({path,std::bind(function_deleter)});
    // flushers_.insert({path,function_flusher});

    // std::list<int> t;
    // for (int i = 0; i < page_nums; ++i) {
    //     t.push_back(i);
    // }
    // free_lists_.insert({path, std::move(t)});
    // replacers_.insert({path, new LRUKReplacer(page_nums, 2)});
    // files_name_.insert(path);
    // if (first_loaded) {
    //     page_id_t id;
    //     auto* page = GetNewPage<PAGE_SIZE>(path,&id);
    //     auto*  d= page->data;
    //     *(uint8_t*)d = (uint8_t)1<<7;
    //     UnPin<PAGE_SIZE>(path,page->page_id,true);
    //     //TODO(WXY):the first page use to bitmap for allocate page.
    //     page_counts_[path] = 1;
    // } else {

    //     page_counts_[path] = size / PAGE_SIZE;
    // }

    // backgroud_flush_thread_ = std::thread([this](){
    //     std::mutex lock_;
    //     while(!finished_){
    //         std::unique_lock<std::mutex> l(lock_);
    //         this->flusher_cv_.wait_for(l,std::chrono::seconds(1));
    //         for(auto& it : flushers_ ){
    //             it.second();
    //         }
    //     }
    // });

    auto* disk= std::any_cast<DiskManager<PAGE_SIZE>*>(disk_managers_[path]);

    auto load_f = std::bind(&DiskManager<PAGE_SIZE>::ReadPage,disk,std::placeholders::_1,std::placeholders::_2);

    return {FileHandle<PAGE_SIZE>(path, *this,load_f),is_first_load};
}



template<uint32_t PAGE_SIZE>
Page<PAGE_SIZE> *
FileManager::GetPage(const std::string &path, page_id_t page_id) {
    auto* disk = std::any_cast<DiskManager<PAGE_SIZE>*>(disk_managers_[path]);
    return  disk->GetPage(page_id);
    //TODO(wxy):check if allocated
    // std::unique_lock<std::mutex> l(locks_[path]);
    // if(page_counts_[path] <= page_id){
    //     throw "try get a page that not allocated";
    // }

    // auto* any = buffer_pools_[path];
    // auto* pages = reinterpret_cast<Page<PAGE_SIZE>*>(any);
    // auto *replacer = replacers_[path];
    // auto &free_list = free_lists_[path];
    // auto &map = maps_[path];
    // auto &file = files_[path];

    // auto it =
    //     std::find_if(std::begin(map), std::end(map), [&](auto &it) -> bool {
    //         if (it.first == page_id) {
    //             return true;
    //         }
    //         return false;
    //     });
    // frame_id_t victim_idx;
    // if (it == std::end(map)) {
    //     if (!free_list.empty()) {
    //         auto idx = free_list.front();
    //         free_list.pop_front();
    //         auto *page = &pages[idx];
    //         ReadPage<PAGE_SIZE>(file, path, page_id, page->GetData());
    //         assert(page->pin_ == 0);
    //         page->pin_= 1;
    //         page->page_id = page_id;
    //         page->is_dirty = false;
    //         replacer->RecordAccess(idx);
    //         map.insert({page_id, idx});
    //         replacer->SetEvictable(idx, false);
    //         return page;
    //     }
    //     if (replacer->Evict(&victim_idx)) {
    //         Page<PAGE_SIZE>* page = &pages[victim_idx];
    //         if (page->is_dirty) {
    //             FlushPage<PAGE_SIZE>(file, path, page->page_id, map);
    //         }
    //         page->ResetMemory();
    //         page->is_dirty = false;
    //         page->pin_=1;
    //         map.erase(page->page_id);
    //         page->page_id = page_id;
    //         ReadPage<PAGE_SIZE>(file, path, page_id, page->GetData());
    //         map.insert({page_id, victim_idx});
    //         replacer->RecordAccess(victim_idx);
    //         replacer->SetEvictable(victim_idx, false);
    //         return page;
    //     }
    //     throw "Evict Fail";
    // }
    // auto *page = &pages[map[page_id]];
    // page->pin_++;
    // replacer->RecordAccess(map[page_id]);
    // replacer->SetEvictable(map[page_id], false);
    // return page;
}

template<uint32_t PAGE_SIZE>
void
FileManager::UnPin(const std::string &path, page_id_t page_id, bool is_dirty) {

    auto* disk = std::any_cast<DiskManager<PAGE_SIZE>*>(disk_managers_[path]);
    disk->Unpin(page_id,is_dirty);

    // std::unique_lock<std::mutex> l(locks_[path]);
    // auto &map = maps_[path];
    // auto &replacer = replacers_[path];
    // auto it = std::find_if(std::begin(map), std::end(map), [&](auto &it) {
    //     if (it.first == page_id) {
    //         return true;
    //     }
    //     return false;
    // });

    // if (it != std::end(map)) {
    //     auto* page = reinterpret_cast<Page<PAGE_SIZE>*>(buffer_pools_[path])+it->second;
    //     if (page->pin_ >= 1) {
    //         page->pin_--;
    //         if (page->pin_ == 0) {
    //             replacer->SetEvictable(it->second, true);
    //         }
    //         //WTF?
    //         auto result = ( page->is_dirty == false ?   is_dirty  : true);
    //         page->is_dirty = result;
    //         return;
    //     } else {
    //         throw "The page unpined value pin =0 ?";
    //     }
    // }
    // throw "you unpin a page that not loaded";
}

// template<uint32_t PAGE_SIZE>
// void
// FileManager::ReadPage(std::fstream &f, const std::string &path,
//                       page_id_t page_id, char *page_data) {
//     int offset = page_id * PAGE_SIZE;

//     if (offset > GetFileSize(f)) {
//         throw "read past of the file ?";
//     }
//     f.seekp(offset);
//     f.read(page_data, PAGE_SIZE);
//     if (f.bad()) {
//         throw "IO error";
//     }

//     int read_count = f.gcount();
//     if (read_count < PAGE_SIZE) {
//         throw "READ LESS THAN A PAGE";
//     }
// }



int
GetFileSize(std::fstream &f) {
    f.seekg(0,std::ios::end);
    return f.tellg();
}

// template<uint32_t PAGE_SIZE>
// void
// FileManager::FlushPage(std::fstream &f, const std::string &path,
//                        page_id_t page_id,
//                        std::unordered_map<page_id_t, frame_id_t> &map) {
//     auto it = std::find_if(std::begin(map), std::end(map), [&](auto &it) {
//         if (it.first == page_id) {
//             return true;
//         }
//         return false;
//     });
//     assert(it != std::end(map));
//     auto *page = reinterpret_cast<Page<PAGE_SIZE>*>(buffer_pools_[path])+it->second;
//     WritePage<PAGE_SIZE>(f, path, page_id, page->GetData());
//     page->is_dirty = false;
// }

// template<uint32_t PAGE_SIZE>
// void
// FileManager::WritePage(std::fstream &f, const std::string &path,
//                        page_id_t page_id, const char *page_data) {
        
//     int offset = page_id * PAGE_SIZE;
//     f.seekp(offset);
//     f.write(page_data, PAGE_SIZE);
//     if (f.bad()) {
//         throw "error when write page";
//     }
//     f.flush();
// }

template<uint32_t PAGE_SIZE>
Page<PAGE_SIZE> *
FileManager::GetNewPage(const std::string &path, page_id_t *page_id) {
    auto* disk = std::any_cast<DiskManager<PAGE_SIZE>*>(disk_managers_[path]);    
    return disk->GetNewPage(page_id);
}

FileManager::~FileManager() {
    std::lock_guard<std::mutex> l(lock_);
    for (auto it : deleters_) {
        it.second();
    }
}


LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k) {
    max_size_ = num_frames;
}

auto
LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {

    std::lock_guard<std::mutex> m(latch_);
    if (Size() == 0) {
        return false;
    }

    for (auto it = new_frame_.rbegin(); it != new_frame_.rend(); ++it) {

        auto frame = *it;
        if (evictable_[frame]) {
            recorder_[frame] = 0;
            new_locate_.erase(frame);
            new_frame_.remove(frame);

            *frame_id = frame;
            curr_size_--;

            hist_[frame].clear();
            return true;
        }
    }
    // delete page that approve K reference.
    for (auto it = cache_frame_.begin(); it != cache_frame_.end(); ++it) {
        auto frame = it->first;

        if (evictable_[frame]) {
            recorder_[frame] = 0;
            cache_frame_.erase(it);
            cache_locate_.erase(frame);

            *frame_id = frame;

            curr_size_--;
            hist_[frame].clear();
            return true;
        }
    }

    return false;
}

void
LRUKReplacer::RecordAccess(frame_id_t frame_id) {

    std::unique_lock<std::mutex> m(latch_);

    if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
        return;
    }

    recorder_[frame_id]++;
    current_timestamp_++;
    hist_[frame_id].push_back(current_timestamp_);

    auto count = recorder_[frame_id];

    if (count == 1) {
        // new add page

        if (curr_size_ == max_size_) {
            m.unlock();
            frame_id_t frame;
            Evict(&frame);
            m.lock();
        }
        new_frame_.push_front(frame_id);
        new_locate_[frame_id] = new_frame_.begin();
        curr_size_++;
        evictable_[frame_id] = true;
    }

    if (count == k_) {
        // move from new_frome to cache_frame
        new_frame_.remove(frame_id);
        new_locate_.erase(frame_id);

        // get the Kth time
        auto kth_time = hist_[frame_id].front();
        k_time new_cache(frame_id, kth_time);

        auto it = std::upper_bound(cache_frame_.begin(), cache_frame_.end(),
                                   new_cache, CmpKtime);
        it = cache_frame_.insert(it, new_cache);
        cache_locate_[frame_id] = it;
        return;
    }

    if (count > k_) {
        hist_[frame_id].erase(hist_[frame_id].begin());
        cache_frame_.erase(cache_locate_[frame_id]);
        auto kth_time = hist_[frame_id].front();
        k_time new_cache(frame_id, kth_time);
        auto it = std::upper_bound(cache_frame_.begin(), cache_frame_.end(),
                                   new_cache, CmpKtime);   // 找到该插入的位置
        it = cache_frame_.insert(it, new_cache);
        cache_locate_[frame_id] = it;
    }
    // if(count <k_){
    //     // LRU replace poly
    //     new_frame_.remove(frame_id);
    //     new_frame_.push_front(frame_id);
    //     new_locate_[frame_id]=new_frame_.begin();

    //}
}

void
LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {

    std::lock_guard<std::mutex> m(latch_);
    if (recorder_[frame_id] == 0) {
        return;
    }

    if (evictable_[frame_id] && !set_evictable) {
        curr_size_--;
        max_size_--;
    }
    if (!evictable_[frame_id] && set_evictable) {
        curr_size_++;
        max_size_++;
    }
    evictable_[frame_id] = set_evictable;
}

void
LRUKReplacer::Remove(frame_id_t frame_id) {
    if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
        return;
    }
    auto count = recorder_[frame_id];
    if (count == 0) {
        return;
    }

    if (!evictable_[frame_id]) {
        return;
    }

    if (count < k_) {
        // delete from new_frame_

        curr_size_--;
        new_frame_.remove(frame_id);
        new_locate_.erase(frame_id);

        recorder_[frame_id] = 0;
        hist_[frame_id].clear();

    } else {
        // delete from cache_frame_

        cache_frame_.erase(cache_locate_[frame_id]);
        cache_locate_.erase(frame_id);
        curr_size_--;
        recorder_[frame_id] = 0;
        hist_[frame_id].clear();
    }
}

auto
LRUKReplacer::Size() -> size_t {
    return curr_size_;
}
