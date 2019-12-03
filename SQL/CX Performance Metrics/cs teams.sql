create table D_POST_INSTALL.T_CX_TEAMS
(
    DEPARTMENT_NAME       VARCHAR,
    DEPARTMENT_SHORT_NAME VARCHAR,
    DEPARTMENT_ID         NUMBER,
    TEAM_NAME             VARCHAR,
    TEAM_ID               NUMBER,
    FULL_TEAM_NAME        VARCHAR,
    START_DATE            DATE,
    END_DATE              DATE,
    IS_AGENT              BOOLEAN
)
    stage_file_format =
(
    REPLACE_INVALID_CHARACTERS =
    false,
    SKIP_BLANK_LINES =
    false
);

