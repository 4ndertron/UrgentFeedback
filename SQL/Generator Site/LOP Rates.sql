SELECT U.STATE
     , U.UTILITY
     , NVL(U.BLENDED_RATE, U.VSLR_CALCULATED_RATE)              AS CALCULATED_RATE
     , NVL(U.EIA_RATE_LAST_VERIFIED, U.VSLR_CALC_LAST_VERIFIED) AS LAST_VERIFIED_DATE
FROM D_POST_INSTALL.T_UTILITY_RATES_SUMMARY AS U