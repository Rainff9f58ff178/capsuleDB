






#include <ios>
#include <iostream>
#include"./src/Storage/BufferManager.h"
#include<cassert>
using namespace std;

void test_store(){
    
    auto file = std::make_shared<FileManager>();
    auto [handle,_2] = file->open<4096>("./test-buffer-manager-test-read",std::ios::in | std::ios::out | std::ios::binary,10 );
    page_id_t page_id;
    auto* page = handle.GetNewPage(&page_id);
    assert(page->page_id==0);
    std::string s = "hello wrold\\0";
    memcpy(page->data,s.data(),s.size());
    handle.Unpin(page->page_id,true);
}
void test_read(){
    auto file = std::make_shared<FileManager>();
    auto [handle,_2] = file->open<4096>("./test-buffer-manager-test-read",std::ios::in | std::ios::out | std::ios::binary,10 );
    auto* page = handle.GetPage(0);
    cout<<page->data<<endl;
    handle.Unpin(page->page_id,false);
}
void test_disk(){
    test_store();
    test_read();
}


int main(){
    
    return 0;
}