COPY INTO D_POST_INSTALL.T_ERT_ROOT_CAUSE_TAGS
    FROM @MY_UPLOADER_STAGE/root_cause_upload.csv.gz
    file_format = (format_name = D_POST_INSTALL.CX_CSV_WITH_HEADER)
    on_error = 'skip_file';
-- Number of columns in file (50) does not match that of the corresponding table (9), use file format option
-- error_on_column_count_mismatch=false to ignore this error

SELECT * FROM D_POST_INSTALL.T_ERT_ROOT_CAUSE_TAGS;
-- DELETE FROM D_POST_INSTALL.T_ERT_ROOT_CAUSE_TAGS WHERE SERVICE_NUMBER IS NOT NULL;