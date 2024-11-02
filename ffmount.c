#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>  // Add this line to include the errno header

// Function to write data to a file
size_t write_data(void *ptr, size_t size, size_t nmemb, FILE *stream) {
    return fwrite(ptr, size, nmemb, stream);
}

// Function to download a single file using libcurl
int download_file(const char *url, const char *local_path) {
    CURL *curl;
    FILE *fp;
    CURLcode res;

    // Initialize curl
    curl = curl_easy_init();
    if (curl) {
        fp = fopen(local_path, "wb");
        if (!fp) {
            fprintf(stderr, "Failed to open file: %s\n", local_path);
            return -1;
        }

        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);

        // Perform the request, res will get the return code
        res = curl_easy_perform(curl);
        if (res != CURLE_OK) {
            fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
            fclose(fp);
            curl_easy_cleanup(curl);
            return -1;
        }

        // Clean up and close file
        fclose(fp);
        curl_easy_cleanup(curl);
        return 0;
    } else {
        fprintf(stderr, "Failed to initialize curl\n");
        return -1;
    }
}

// Function to create local directories if they don't exist
void create_directory(const char *path) {
    if (mkdir(path, 0755) == -1) {
        if (errno != EEXIST) {
            perror("Error creating directory");
            exit(EXIT_FAILURE);
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <s3_url> <local_directory>\n", argv[0]);
        return 1;
    }

    const char *s3_url = argv[1];
    const char *local_directory = argv[2];

    // Create the local directory if it doesn't exist
    create_directory(local_directory);

    // Example usage: Download a sample file for demonstration
    // (Replace this part with logic to iterate over the list of files in the bucket)
    char file_url[1024];
    snprintf(file_url, sizeof(file_url), "%s/sample-file.txt", s3_url);

    char local_file_path[1024];
    snprintf(local_file_path, sizeof(local_file_path), "%s/sample-file.txt", local_directory);

    printf("Downloading from %s to %s\n", file_url, local_file_path);
    if (download_file(file_url, local_file_path) == 0) {
        printf("Download successful\n");
    } else {
        printf("Download failed\n");
    }

    return 0;
}
