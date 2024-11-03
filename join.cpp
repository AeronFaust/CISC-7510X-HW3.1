#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>

using namespace std;

// Split a line by a delimiter (e.g., comma)
vector<string> split(const string& line, char delimiter = ',') {
    vector<string> result;
    stringstream ss(line);
    string item;
    while (getline(ss, item, delimiter)) {
        result.push_back(item);
    }
    return result;
}

// Read a CSV file into a vector of vectors (2D array)
vector<vector<string>> read_csv(const string& filename) {
    ifstream file(filename);
    vector<vector<string>> csv_data;
    string line;

    if (!file.is_open()) {
        cerr << "Error: Unable to open file " << filename << endl;
        exit(EXIT_FAILURE);
    }

    while (getline(file, line)) {
        csv_data.push_back(split(line));
    }
    return csv_data;
}

// Write the result to stdout
void write_csv(const vector<vector<string>>& result) {
    for (const auto& row : result) {
        for (size_t i = 0; i < row.size(); ++i) {
            cout << row[i];
            if (i != row.size() - 1) {
                cout << ",";
            }
        }
        cout << endl;
    }
}

vector<vector<string>> inner_loop_join(const vector<vector<string>>& csv1, const vector<vector<string>>& csv2, int key_col) {
    vector<vector<string>> result;

    for (const auto& row1 : csv1) {
        for (const auto& row2 : csv2) {
            if (row1[key_col] == row2[key_col]) {
                vector<string> joined_row = row1;
                joined_row.insert(joined_row.end(), row2.begin() + 1, row2.end());
                result.push_back(joined_row);
            }
        }
    }
    return result;
}

vector<vector<string>> hash_join(const vector<vector<string>>& csv1, const vector<vector<string>>& csv2, int key_col) {
    unordered_map<string, vector<vector<string>>> hash_table;
    vector<vector<string>> result;

    // Build phase: build hash table for csv1
    for (const auto& row1 : csv1) {
        hash_table[row1[key_col]].push_back(row1);
    }

    // Probe phase: probe hash table with csv2
    for (const auto& row2 : csv2) {
        if (hash_table.count(row2[key_col])) {
            for (const auto& matching_row : hash_table[row2[key_col]]) {
                vector<string> joined_row = matching_row;
                joined_row.insert(joined_row.end(), row2.begin() + 1, row2.end());
                result.push_back(joined_row);
            }
        }
    }
    return result;
}

vector<vector<string>> merge_join(vector<vector<string>>& csv1, vector<vector<string>>& csv2, int key_col) 
{
    vector<vector<string>> result;
    sort(csv1.begin(), csv1.end(), [key_col](const vector<string>& a, const vector<string>& b) {
        return a[key_col] < b[key_col];
    });
    sort(csv2.begin(), csv2.end(), [key_col](const vector<string>& a, const vector<string>& b) {
        return a[key_col] < b[key_col];
    });

    size_t i = 0, j = 0;
    while (i < csv1.size() && j < csv2.size()) {
        if (csv1[i][key_col] < csv2[j][key_col]) {
            i++;
        } else if (csv1[i][key_col] > csv2[j][key_col]) {
            j++;
        } else {
            vector<string> joined_row = csv1[i];
            joined_row.insert(joined_row.end(), csv2[j].begin() + 1, csv2[j].end());
            result.push_back(joined_row);
            i++;
            j++;
        }
    }
    return result;
}

int main(int argc, char* argv[]) 
{
    if (argc < 5) 
    {
        cerr << "Usage: " << argv[0] << " <file1.csv> <file2.csv> <join_type> <key_column>" << endl;
        cerr << "Join types: inner_loop, hash, merge" << endl;
        return 1;
    }

    string file1 = argv[1];
    string file2 = argv[2];
    string join_type = argv[3];
    int key_col = stoi(argv[4]);

    // Read the CSV files
    vector<vector<string>> csv1 = read_csv(file1);
    vector<vector<string>> csv2 = read_csv(file2);

    vector<vector<string>> result;

    // Choose the join type
    if (join_type == "inner_loop") 
    {
        result = inner_loop_join(csv1, csv2, key_col);
    } 
    else if (join_type == "hash") 
    {
        result = hash_join(csv1, csv2, key_col);
    } 
    else if (join_type == "merge") 
    {
        result = merge_join(csv1, csv2, key_col);
    } 
    else 
    {
        cerr << "Error: Unknown join type '" << join_type << "'." << endl;
        return 1;
    }

    // Output the result to stdout
    write_csv(result);

    return 0;
}
