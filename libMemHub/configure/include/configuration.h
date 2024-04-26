#ifndef CONFIGURATION_H
#define CONFIGURATION_H

#include <string>
#include <libconfig.h++>
#include <unordered_map>

class CConfiguration
{
public:
    explicit CConfiguration(const std::string &config_path);

    std::string get_string_entry(const std::string &key);

    int get_int_entry(const std::string &key);

    bool get_bool_entry(const std::string &key);

    void get_node_ip_port(std::unordered_map<int, std::unordered_map<std::string, std::string>> *ip_port_info);

    int get_max_check_num();
    int get_max_reply_num();

private:
    libconfig::Config cfg;

    bool profiling_enabled;
};

#endif