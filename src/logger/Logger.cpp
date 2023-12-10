#include "Logger.h"
#include<format>
#include<sstream>
#include<iomanip>


void Logger::AppendDEBUG(const std::string& info){
    char buffer[128];
    time_t t = time(NULL);
    struct tm * p = localtime(&t);
    
    strftime(buffer, sizeof buffer, "%d %Y %c", p);
    std::string _info = buffer;
    std::stringstream ss;
    ss << _info
    << std::this_thread::get_id()
    << std::setw(10)
    << "DEBUG ["
    <<info
    <<" ]   ";

    std::unique_lock<std::mutex> lock(internal_lock_);
    queue_.push_back(std::move(ss.str()));
    cv_.notify_one();
}



Logger::Logger(const std::string& log_name){
    std::fstream f;
    
    f.open("./"+log_name, std::ios::in | std::ios::out | std::ios::ate);

    if (!f.is_open()) {
        f.open("./"+log_name, std::ios::in| std::ios::out | std::ios::trunc | std::ios::ate);
    }

    f_= std::move(f);
    queue_.push_back("===========================================BEGIN===========================================");
    flush_thread_ = std::thread([&,this](){
        //singal thread flush log , enough
        while(1 ){
            std::unique_lock<std::mutex> lock(internal_lock_);

            cv_.wait(lock,[&,this](){
                if(finished_)
                    return true;
                return  !queue_.empty();
            });

            if(finished_)
                break;

            auto s = std::move(queue_.front());
            queue_.pop_front();
            lock.unlock();
            f_<<s<<std::endl;
            f_.flush();
            
        }
        
    });


}