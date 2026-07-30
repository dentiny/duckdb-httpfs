target_sources(
  httpfs_library
  PRIVATE s3_auth.cpp
          s3_delete.cpp
          s3_list.cpp
          s3_multi_part_upload.cpp
          s3_request.cpp
          s3_settings.cpp
          s3_url.cpp
          s3fs.cpp)
