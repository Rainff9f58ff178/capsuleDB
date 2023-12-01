#pragma once 

#include "BufferManager.h"
#include<iostream>
#define EXHASH_TEMPLATE template <class Key, class Value>
#define EXHASH ExtensiableHash<Key, Value>

EXHASH_TEMPLATE
class ExtensiableHash {
private:
  FileHandle ghi_file_handle_;
  FileHandle ghd_file_handle_;
  int ghi_file_page_num_;
  int ghd_file_page_num_;

  FileManager* file_manager_;
  DirectoryHeader *ghi_meta_header_page_;
  int global_depth_;
  Page *ghd_meta_header_page_;

  // one directory page could store bucket addr
  static int constexpr SLOT_ONE_DIR_PAGE = BUCKET_PAGE_SIZE/4;

public:
  explicit ExtensiableHash(const std::string &path, FileManager* manager,
                           int ghi_page_nums, int ghd_page_nums);
  DISALLOW_COPY_AND_MOVE(ExtensiableHash);
  ~ExtensiableHash();

  bool Insert(const Key &key, const Value &value);
  int Find(const Key& key,std::vector<std::pair<Key,Value>>* vec);

  void Summary();
private:
  int IndexOf(const Key &key);
  void EnlargerDirectory();
};