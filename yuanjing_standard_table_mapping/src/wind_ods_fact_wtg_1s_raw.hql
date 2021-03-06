use bi;
create external table wind_ods_fact_wtg_1s_raw(
COMMON_Reserved string,
COMMON_ProtocolID string,
COMMON_WTGID string,
COMMON_DateTime string,
COMMON_Version string,
WCNV_CosCon double,
WCNV_CurConL1 double,
WCNV_CurConL2 double,
WCNV_CurConL3 double,
WCNV_CurGrid double,
WCNV_CurMotor double,
WCNV_GridFreq double,
WCNV_GridRePW double,
WCNV_InvPhase1Tmp double,
WCNV_InvPhase2Tmp double,
WCNV_InvPhase3Tmp double,
WCNV_OCCutIn double,
WCNV_TemConv2 double,
WCNV_TemOutCoolSys double,
WCNV_TempINUIGBTMax double,
WCNV_TempISUIGBTMax double,
WCNV_TempTrfmOut double,
WCNV_TmpInvRotU double,
WCNV_TmpInvRotV double,
WCNV_TmpInvRotW double,
WCNV_Torque double,
WCNV_TorqueSetpoint double,
WCNV_VolConL1 double,
WCNV_VolConL2 double,
WCNV_VolConL3 double,
WGEN_ActivePWCal double,
WGEN_GenActivePW double,
WGEN_GENPreWaterCoolInlet double,
WGEN_GENPreWaterCoolOutlet double,
WGEN_GenReactivePW double,
WGEN_GenSenMaxTmp double,
WGEN_GenSenTmp1 double,
WGEN_GenSenTmp2 double,
WGEN_GenSenTmp3 double,
WGEN_GenSenTmp4 double,
WGEN_GenSenTmp5 double,
WGEN_GenSenTmp6 double,
WGEN_GenSpd double,
WGEN_GENTemWaterCoolInlet double,
WGEN_GENTemWaterCoolOutlet double,
WGEN_TemGenDriEnd double,
WGEN_TemGenNonDE double,
WGEN_TemGenStaU double,
WGEN_TemGenStaU2 double,
WGEN_TemGenStaV double,
WGEN_TemGenStaV2 double,
WGEN_TemGenStaW double,
WGEN_TemGenStaW2 double,
WGEN_CoolTemoutGen double,
WNAC_AirPressure double,
WNAC_CtlCabTmp double,
WNAC_HumNacelle double,
WNAC_TemNacelle double,
WNAC_TemNacelleCab double,
WNAC_TemOut double,
WNAC_TopBoxTmp double,
WNAC_WindDirection double,
WNAC_WindSpeed double,
WNAC_WindSpeedMode double,
WNAC_WindVaneDirection double,
WNAC_CoolPumpOutTem double,
WNAC_NacellePosition double,
WROT_BatVolBalde1 double,
WROT_BatVolBalde2 double,
WROT_BatVolBalde3 double,
WROT_Blade1Position double,
WROT_Blade1Speed double,
WROT_Blade2Position double,
WROT_Blade2Speed double,
WROT_Blade3Position double,
WROT_Blade3Speed double,
WROT_BladePosition double,
WROT_BladeTipPres double,
WROT_CurBlade1Motor double,
WROT_CurBlade2Motor double,
WROT_CurBlade3Motor double,
WROT_PowerStorePres double,
WROT_PowerStoreTmp double,
WROT_PtCptTmpBl1 double,
WROT_PtCptTmpBl2 double,
WROT_PtCptTmpBl3 double,
WROT_PtMotorTmpBl1 double,
WROT_PtMotorTmpBl2 double,
WROT_PtMotorTmpBl3 double,
WROT_SlipRingTmp double,
WROT_TemAxisCtrl1 double,
WROT_TemAxisCtrl2 double,
WROT_TemAxisCtrl3 double,
WROT_TemB1Cap double,
WROT_TemB1Mot double,
WROT_TemB2Cap double,
WROT_TemB2Mot double,
WROT_TemB3Cap double,
WROT_TemB3Mot double,
WROT_TemBlade1Inver double,
WROT_TemBlade2Inver double,
WROT_TemBlade3Inver double,
WROT_TemHub double,
WROT_TemPitchBat1 double,
WROT_TemPitchBat2 double,
WROT_TemPitchBat3 double,
WROT_VolB1Cap double,
WROT_VolB2Cap double,
WROT_VolB3Cap double,
WTOW_TemTower double,
WTOW_TemTowerCab double,
WTOW_TemTowerOut double,
WTRF_TempGCCTrfm double,
WTRF_TempTransfPh1 double,
WTRF_TempTransfPh2 double,
WTRF_TempTransfPh3 double,
WTRM_CoolTemoutGBX double,
WTRM_DiffPFilterIn double,
WTRM_GearBoxDistri double,
WTRM_PGOilFilterIn double,
WTRM_PowerStoreTRBS double,
WTRM_PrePowerStoreTRBS double,
WTRM_PresGBoxIn double,
WTRM_RotorSpd double,
WTRM_RotorSpdPrcs double,
WTRM_TemGBoxOilE double,
WTRM_TemGeaHS1 double,
WTRM_TemGeaHS2 double,
WTRM_TemGeaHS3 double,
WTRM_TemGeaMS1 double,
WTRM_TemGeaMS2 double,
WTRM_TemGeaMS3 double,
WTRM_TemGeaMSDE double,
WTRM_TemGeaMSND double,
WTRM_TemGeaOil double,
WTRM_TemGeaZSDE double,
WTRM_TemGeaZSND double,
WTRM_TemMainBearing double,
WTRM_TemMainBearing2 double,
WTUR_AIStatusCode double,
WTUR_LimitPowerSts double,
WTUR_RemoteControlSts double,
WTUR_Reset double,
WTUR_ServiceState double,
WTUR_Start double,
WTUR_StatusCode double,
WTUR_Stop double,
WTUR_SetTurbOp double,
WTUR_TurbineSts double,
WVIB_VibrationL double,
WVIB_VibrationLFil double,
WVIB_VibrationV double,
WVIB_VibrationVFil double,
WWPP_APConsumed double,
WWPP_APProduction double,
WWPP_RPConsumed double,
WWPP_RPProduction double,
WYAW_OTYawCCW double,
WYAW_OTYawCW double,
WYAW_TotalTwist double,
WYAW_YawCCWSts double,
WYAW_YawCWSts double,
WYAW_YawSts double
) partitioned by (hp_date string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/hive/warehouse/bi.db/wind_ods_fact_wtg_1s_raw';
