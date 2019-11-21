COPY INTO D_POST_INSTALL.T_ERT_ROOT_CAUSE_TAGS
    FROM @MY_UPLOADER_STAGE/transfer_account_upload_stage.csv.gz
    file_format = (format_name = D_POST_INSTALL.CX_CSV_WITH_HEADER)
    on_error = 'skip_file';