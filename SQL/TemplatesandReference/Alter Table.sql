BEGIN;

ALTER TABLE -- IF EXISTS
    D_POST_INSTALL.T_ERT_ROOT_CAUSE_TAGS
RENAME TO
    D_POST_INSTALL.T_CX_ROOT_CAUSE_TAGS
;

COMMIT;