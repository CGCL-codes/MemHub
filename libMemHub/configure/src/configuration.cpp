#include <iostream>
#include <sstream>
#include <map>
#include <vector>
#include <cstdlib>
#include <string.h>
#include "../include/configuration.h"

CConfiguration::CConfiguration(const std::string &config_path)
{
    try
    {
        cfg.readFile(config_path.c_str());
    }
    catch (const libconfig::FileIOException &fioex)
    {
        throw std::runtime_error("I/O error while reading config file.");
    }
    catch (const libconfig::ParseException &pex)
    {
        std::ostringstream error;
        error << "Parse error at " << pex.getFile() << ":" << pex.getLine()
              << " - " << pex.getError() << std::endl;
        throw std::runtime_error(error.str());
    }
    const char *hdmlp_profiling = std::getenv("HDMLPPROFILING");
    profiling_enabled = !(!hdmlp_profiling || strcmp(hdmlp_profiling, "0") == 0 || strcmp(hdmlp_profiling, "false") == 0);
}

std::string CConfiguration::get_string_entry(const std::string &key)
{
    std::string val;
    try
    {
        val = cfg.lookup(key).c_str();
    }
    catch (const libconfig::SettingNotFoundException &nfex)
    {
        val = "";
    }
    return val;
}

int CConfiguration::get_int_entry(const std::string &key)
{
    int val;
    try
    {
        val = cfg.lookup(key);
    }
    catch (const libconfig::SettingNotFoundException &nfex)
    {
        val = -1;
    }
    return val;
}

bool CConfiguration::get_bool_entry(const std::string &key)
{
    bool val;
    try
    {
        val = cfg.lookup(key);
    }
    catch (const libconfig::SettingNotFoundException &nfex)
    {
        val = false;
    }
    return val;
}

/**
 * Retrieves the IP address and port information for all training nodes from the configuration file.
 *
 */
void CConfiguration::get_node_ip_port(std::unordered_map<int, std::unordered_map<std::string, std::string>> *ip_port_info)
{
    const libconfig::Setting &root = cfg.getRoot();
    const libconfig::Setting &node_ip_port = root["node_ip_port"];
    int count = node_ip_port.getLength();
    for (int i = 0; i < count; i++)
    {
        const libconfig::Setting &ip_port = node_ip_port[i];
        int rank = ip_port.lookup("rank");
        std::string ip = ip_port.lookup("ip").c_str();
        std::string port = ip_port.lookup("port");
        (*ip_port_info)[rank]["ip"] = ip;
        (*ip_port_info)[rank]["port"] = port;
    }
}

/**
 * Retrieves the maximum number of data to check during target node prefetching when performing cross-node sharing.
 *
 * @return The maximum number of data to check.
 */
int CConfiguration::get_max_check_num()
{
    return this->get_int_entry("max_check_num");
}

/**
 * Retrieves the maximum amount of data to prefetch on the target node during cross-node sharing.
 *
 * @return The maximum amount of data to prefetch.
 */
int CConfiguration::get_max_reply_num()
{
    return this->get_int_entry("max_reply_num");
}
