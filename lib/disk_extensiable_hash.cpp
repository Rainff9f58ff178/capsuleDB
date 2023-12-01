#include "disk_extensiable_hash.h"
#include <cassert>

EXHASH_TEMPLATE
EXHASH::ExtensiableHash(const std::string &path, FileManager* manager,
                        int ghi_page_nums, int ghd_page_nums)
    : file_manager_(manager) {
    ghi_file_handle_ = file_manager_->open(
        path + ".ghi", std::ios::in | std::ios::out | std::ios::binary,
        ghi_page_nums);

    ghd_file_handle_ = file_manager_->open(
        path + ".ghd", std::ios::in | std::ios::out | std::ios::binary,
        ghd_page_nums);

    // if (!ok || !ok2) {
    //     throw "init file failed";
    // }

    ghi_file_page_num_ = ghi_file_handle_.GetPageNum();
    ghd_file_page_num_ = ghd_file_handle_.GetPageNum();
    

    if (ghi_file_page_num_ == 0) {
        // first open
        assert(ghd_file_page_num_ == 0);
        int page_id;
        auto *page = ghi_file_handle_.GetNewPage(&page_id);
        ghi_meta_header_page_ = reinterpret_cast<DirectoryHeader *>(page);
        // create first directory
        auto *first_dir_page = reinterpret_cast<DirectoryPage *>(
            ghi_file_handle_.GetNewPage(&page_id));

        assert(ghi_file_handle_.GetPageNum() == 2);
        ghi_file_page_num_=2;
        ghi_meta_header_page_->SetPageNum(2);
        ghi_meta_header_page_->SetKeySize(sizeof(Key));
        ghi_meta_header_page_->SetValueSize(sizeof(Value));
        ghi_meta_header_page_->SetGlobalDepth(
            DirectoryHeader::INITCIAL_GLOBAL_DEPTH);
        global_depth_ = DirectoryHeader::INITCIAL_GLOBAL_DEPTH;

        ghd_meta_header_page_ = ghd_file_handle_.GetNewPage(&page_id);
        // ghd header not store anything for now .
        ghd_file_page_num_++;
        for (int i = 0; i < BUCKET_PAGE_SIZE / 4; ++i) {
            auto *bucket_page = reinterpret_cast<BucketPage<Key, Value> *>(
                ghd_file_handle_.GetNewPage(&page_id));
            assert(bucket_page->page_id == i + 1);
            bucket_page->SetSCN(0);
            bucket_page->SetOffset(BucketPage<Key, Value>::HEADER_SIZE);
            bucket_page->SetLocalDepth(DirectoryHeader::INITCIAL_GLOBAL_DEPTH);
            bucket_page->SetSize(0);
            first_dir_page->SetBucketId(i, page_id);
            ghd_file_page_num_++;
            ghd_file_handle_.Unpin(bucket_page->page_id, true);
        }
        ghi_file_handle_.Unpin(first_dir_page->page_id, true);
        
    }else{
        // throw "not impliment load file for now ";
        ghi_meta_header_page_   = reinterpret_cast<DirectoryHeader*>(
            ghi_file_handle_.GetPage(0)
        );
        ghd_meta_header_page_ = ghd_file_handle_.GetPage(0);
        global_depth_  =ghi_meta_header_page_->GetGlobalDepth();
    }
}

EXHASH_TEMPLATE
bool
EXHASH::Insert(const Key &key, const Value &value) {
    assert(sizeof(key) == ghi_meta_header_page_->GetKeySize());
    assert(sizeof(value) == ghi_meta_header_page_->GetValueSize());

    int idx = IndexOf(key);
    int directly_page_num = idx / SLOT_ONE_DIR_PAGE + 1;
    int h = idx % SLOT_ONE_DIR_PAGE;
    int dir_size = ghi_meta_header_page_->GetPageNum() - 1;
    auto *dir_page = reinterpret_cast<DirectoryPage *>(
        ghi_file_handle_.GetPage(directly_page_num));
    auto bucket_id = dir_page->GetBucketId(h);
    auto *bucket_page = reinterpret_cast<BucketPage<Key, Value> *>(
        ghd_file_handle_.GetPage(bucket_id));

    int mask = (1 << bucket_page->GetLocalDepth());

    bool sec = bucket_page->Insert(key, value);


    if (!sec) {
        
        if (bucket_page->GetLocalDepth() == global_depth_) {
            bucket_page->SetLocalDepth(bucket_page->GetLocalDepth() + 1);
            auto it = bucket_page->Begin();
            std::vector<std::pair<Key, Value>> vec;
            for (; it != bucket_page->End(); ++it) {
                vec.push_back(std::move(*it));
            }
            EnlargerDirectory();
            int page_id;
            auto *new_bucket_page = reinterpret_cast<BucketPage<Key, Value> *>(
                ghd_file_handle_.GetNewPage(&page_id));
            new_bucket_page->SetSize(0);
            new_bucket_page->SetLocalDepth(global_depth_);
            new_bucket_page->SetOffset(BucketPage<Key, Value>::HEADER_SIZE);
            new_bucket_page->SetSCN(0);
            for (auto &it : vec) {
                if (std::hash<Key>()(it.first) & mask) {
                    new_bucket_page->Insert(it.first, it.second);
                    bucket_page->remove(it.first, it.second);
                }
            }

            for (int i = 0; i < ghi_meta_header_page_->GetPageNum() - 1; ++i) {
                auto *new_dir_page = reinterpret_cast<DirectoryPage *>(
                    ghi_file_handle_.GetPage( i + 1));

                for (int j = 0; j < BUCKET_PAGE_SIZE / 4; ++j) {
                    auto idx = i * BUCKET_PAGE_SIZE / 4 + j;
                    if (new_dir_page->GetBucketId(j) == bucket_page->page_id &&
                        (idx & mask)) {
                        new_dir_page->SetBucketId(j, new_bucket_page->page_id);
                    }
                }
                ghi_file_handle_.Unpin(new_dir_page->page_id,true);
            }
            ghd_file_handle_.Unpin(new_bucket_page->page_id,true);
            ghd_file_handle_.Unpin(bucket_page->page_id,true);
            ghi_file_handle_.Unpin(dir_page->page_id,false);

        } else {
            // bucket_page local depth smaller than gloabl depth
            int page_id;
            bucket_page->SetLocalDepth(bucket_page->GetLocalDepth() + 1);
            auto *new_bucket_page = reinterpret_cast<BucketPage<Key, Value> *>(
                ghd_file_handle_.GetNewPage(&page_id));
            new_bucket_page->SetSize(0);
            new_bucket_page->SetLocalDepth(bucket_page->GetLocalDepth());
            new_bucket_page->SetOffset(BucketPage<Key, Value>::HEADER_SIZE);
            new_bucket_page->SetSCN(0);
            auto it = bucket_page->Begin();
            std::vector<std::pair<Key, Value>> vec;
            for (; it != bucket_page->End(); ++it) {
                vec.push_back(std::move(*it));
            }
            for (auto &it : vec) {
                if (std::hash<Key>()(it.first) & mask) {
                    new_bucket_page->Insert(it.first, it.second);
                    bucket_page->remove(it.first, it.second);
                }
            }

            for (int i = 0; i < (ghi_meta_header_page_->GetPageNum() - 1);
                 ++i) {
                auto *new_dir_page = reinterpret_cast<DirectoryPage *>(
                    ghi_file_handle_.GetPage( i + 1));

                for (int j = 0; j < BUCKET_PAGE_SIZE / 4; ++j) {
                    auto idx = i * BUCKET_PAGE_SIZE / 4 + j;
                    if (new_dir_page->GetBucketId(j) == bucket_page->page_id &&
                        (idx & mask)) {
                        new_dir_page->SetBucketId(j, new_bucket_page->page_id);
                    }
                }
                ghi_file_handle_.Unpin(new_dir_page->page_id, true);
            }
            ghd_file_handle_.Unpin(new_bucket_page->page_id,true);
            ghd_file_handle_.Unpin(bucket_page->page_id,true);
            ghi_file_handle_.Unpin(dir_page->page_id,false);
        }
        return Insert(key,value);
    }

    ghi_file_handle_.Unpin(dir_page->page_id,false);
    ghd_file_handle_.Unpin(bucket_page->page_id,true);
    
    return true;
}


EXHASH_TEMPLATE
int EXHASH::Find(const Key& key, std::vector<std::pair<Key,Value>>* vec){
    auto idx = IndexOf(key);
    auto page_num = idx/SLOT_ONE_DIR_PAGE+1;
    auto h = idx % SLOT_ONE_DIR_PAGE;
    auto* dir_page = reinterpret_cast<DirectoryPage*>(
        ghi_file_handle_.GetPage(page_num)
    );
    auto bucket_id = dir_page->GetBucketId(h);
    auto* bucket_page=reinterpret_cast<BucketPage<Key,Value>*>(
        ghd_file_handle_.GetPage(bucket_id));

    int num = bucket_page->Find(key, vec);
    ghi_file_handle_.Unpin(page_num,false);
    ghd_file_handle_.Unpin(bucket_page->page_id, false);
    return num;
}


EXHASH_TEMPLATE
void
EXHASH::EnlargerDirectory() {

    auto num = ghi_meta_header_page_->GetPageNum() - 1;
    int page_id;

    auto fill_dir_page = [&](auto *old_dir_page, auto *new_dir_page) {
        memcpy(new_dir_page->GetData(), old_dir_page->GetData(),
               BUCKET_PAGE_SIZE);
    };

    for (int i = 0; i < num; ++i) {
        auto *new_dir_page = ghi_file_handle_.GetNewPage(&page_id);
        auto *old_dir_page = ghi_file_handle_.GetPage(i + 1);
        fill_dir_page(old_dir_page, new_dir_page);
        ghi_file_handle_.Unpin(new_dir_page->page_id,true);
        ghi_file_handle_.Unpin(old_dir_page->page_id,false);
    }
    ghi_meta_header_page_->SetPageNum(num * 2 + 1);
    ghi_meta_header_page_->SetGlobalDepth(
        ghi_meta_header_page_->GetGlobalDepth() + 1);
    global_depth_++;
}

EXHASH_TEMPLATE
void EXHASH::Summary(){
    std::vector<int> bucket_page_ids;
    auto director_page_num = ghi_meta_header_page_->GetPageNum()-1;
    for(int i=0;i<director_page_num;++i){
        auto dir_page = reinterpret_cast<DirectoryPage*>(
            ghi_file_handle_.GetPage(i+1));
        for(int j=0;j<BUCKET_PAGE_SIZE/4;++j){
            auto bucket =dir_page->GetBucketId(j);
            bucket_page_ids.push_back(bucket);
        }
            ghi_file_handle_.Unpin(dir_page->page_id,false);
    }

    for(auto bucket_id : bucket_page_ids){
        auto bucket_page = reinterpret_cast<BucketPage<Key,Value>*>(
            ghd_file_handle_.GetPage(bucket_id));
        
        auto size = bucket_page->GetSize();
        std::cout<<bucket_id<<"nd page"<<", exist "<<size<<" pair"<<std::endl;
        ghd_file_handle_.Unpin(bucket_page->page_id,false);
    }

}


EXHASH_TEMPLATE
EXHASH::~ExtensiableHash() {
    ghi_file_handle_.Unpin(ghi_meta_header_page_->page_id, true);
    ghd_file_handle_.Unpin(ghd_meta_header_page_->page_id, true);
}

EXHASH_TEMPLATE
int
EXHASH::IndexOf(const Key &key) {
    int mask = (1 << global_depth_) - 1;
    return std::hash<Key>()(key) & mask;
}
template class ExtensiableHash<int32_t, int32_t>;
template class ExtensiableHash<uint64_t, uint64_t>;