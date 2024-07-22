# -*- coding: utf-8 -*-
"""
Created on Thu Sep 21 16:23:17 2023

@author: cheppner
"""

RUNOFF_FRACTION = 0.0
INITIAL_SOIL_STOR = 0
SOIL_STOR_CAP = 16


def eff_precip(precip, et):
    return min(precip, et, max(0, (0.70917 * (precip ** 0.82416) - 0.11556) * (10 ** (0.02426 * et))))


def calc_runoff(ppt, frac):
    return ppt * frac


def rem_ppt(ppt, amount):
    return ppt - amount


def soil_stor_before_CU(cap, prev_ss, ppt_after_ro):
    return min(cap, prev_ss + ppt_after_ro)


def CU_of_soil_stor(ss_before_CU, et, eff_ppt):
    return min(ss_before_CU, et - eff_ppt)


def soil_stor_after_CU(ss_before_CU, CU_soil_stor):
    return ss_before_CU - CU_soil_stor


def CU_of_applied_water(et, eff_ppt, CU_ss):
    return et - eff_ppt - CU_ss


def CU_of_precip(eff_ppt, CU_ss):
    return eff_ppt + CU_ss


def smb_calc(precip, et, ro_frac, ss_capacity, prev_ss):
    eff_ppt = eff_precip(precip, et)
    rem_ppt_after_eff_ppt = rem_ppt(precip, eff_ppt)
    ro = calc_runoff(rem_ppt_after_eff_ppt, ro_frac)
    rem_ppt_after_runoff = rem_ppt(rem_ppt_after_eff_ppt, ro)
    ss_before_CU = soil_stor_before_CU(ss_capacity, prev_ss, rem_ppt_after_runoff)
    CU_ss = CU_of_soil_stor(ss_before_CU, et, eff_ppt)
    ss_after_CU = soil_stor_after_CU(ss_before_CU, CU_ss)
    cu_AW = CU_of_applied_water(et, eff_ppt, CU_ss)
    cu_ppt = CU_of_precip(eff_ppt, CU_ss)
    return eff_ppt, ro, CU_ss, ss_after_CU, cu_AW, cu_ppt


def smb_calc_t0(precip, et, ro_frac, ss_capacity, prev_ss):
    eff_ppt = eff_precip(precip, et)
    rem_ppt_after_eff_ppt = rem_ppt(precip, eff_ppt)
    ro = calc_runoff(rem_ppt_after_eff_ppt, ro_frac)
    rem_ppt_after_runoff = rem_ppt(rem_ppt_after_eff_ppt, ro)
    ss_before_CU = prev_ss
    CU_ss = CU_of_soil_stor(ss_before_CU, et, eff_ppt)
    ss_after_CU = soil_stor_after_CU(ss_before_CU, CU_ss)
    cu_AW = CU_of_applied_water(et, eff_ppt, CU_ss)
    cu_ppt = CU_of_precip(eff_ppt, CU_ss)
    return eff_ppt, ro, CU_ss, ss_after_CU, cu_AW, cu_ppt


def calc_SMB_for_time_series(ppt_series,
                             et_series,
                             ):
    ss = []
    ppt_eff = []
    runoff = []
    cons_use_ss = []
    cons_use_AW = []
    cons_use_ppt = []
    for t in range(len(ppt_series)):
        if t == 0:
            eff_ppt, ro, cu_ss, ss_after_CU, cu_AW, cu_ppt = smb_calc_t0(ppt_series[t], et_series[t], RUNOFF_FRACTION,
                                                                         SOIL_STOR_CAP, INITIAL_SOIL_STOR)
            ss.append(ss_after_CU)
            ppt_eff.append(eff_ppt)
            runoff.append(ro)
            cons_use_ss.append(cu_ss)
            cons_use_AW.append(cu_AW)
            cons_use_ppt.append(cu_ppt)
        else:
            eff_ppt, ro, cu_ss, ss_after_CU, cu_AW, cu_ppt = smb_calc(ppt_series[t], et_series[t], RUNOFF_FRACTION,
                                                                      SOIL_STOR_CAP, ss[t - 1])
            ss.append(ss_after_CU)
            ppt_eff.append(eff_ppt)
            runoff.append(ro)
            cons_use_ss.append(cu_ss)
            cons_use_AW.append(cu_AW)
            cons_use_ppt.append(cu_ppt)
    return ss, ppt_eff, runoff, cons_use_ss, cons_use_AW, cons_use_ppt

# ppt_series = [3.058, 0.518, 6.815, 1.893, 0.235, 0, 0.005, 0, 0.015, 0.406, 3.923]
# et_series = [0.869, 1.394, 2.868, 4.429, 3.928, 2.863, 3.044, 2.048, 1.296, 1.044, 0.677]

# ss, ppt_eff, runoff, cons_use_ss, cons_use_AW, cons_use_ppt = calc_SMB_for_time_series(ppt_series, et_series, RUNOFF_FRACTION, SOIL_STOR_CAP, INITIAL_SOIL_STOR)
