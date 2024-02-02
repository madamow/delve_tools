import pandas as pd
import numpy as np

from despydb import DesDbi
dbh = DesDbi(None, 'db-decade', retry=True)
cur = dbh.cursor()


def remove_duplicates(dfd):
    astro = out[cls]
    dupl = out[astro.duplicated(keep='last')]

    if dupl.shape[0] != 0:
        pfws = ",".join([str(e) for e in dupl['PFW_ATTEMPT_ID'].to_list()])
        print("Removing duplicate", pfws)
        query = ("update PROCTAG set tag='FINALCUT_DUPLICATE' "
                 f"where pfw_attempt_id in ({pfws}) and tag='{tag}'")

        cur.execute(query)
        dbh.commit()

    nodup = out[astro.duplicated(keep='last') is False]
    nodup = nodup.reset_index(drop=True)
    return nodup


def remove_bad(dfd):
    out_bad = dfd[(dfd['SIGMA_REF'] >= 1.) |
                  (dfd['NDETS_REF'] < 100.)]['PFW_ATTEMPT_ID'].to_list()
    if out_bad:
        print("Removing", out_bad)
        pfws = ",".join([str(e) for e in out_bad])
        query = ("update PROCTAG set tag='BAD_ASTROM' "
                 f"where pfw_attempt_id in ({pfws})")
        cur.execute(query)
        dbh.commit()
    good = dfd[(dfd['SIGMA_REF'] < 1.) & (dfd['NDETS_REF'] > 100.)]
    good = good.reset_index(drop=True)
    return good


def sqsum(x1, x2):
    return np.sqrt((x1*x1) + (x2*x2))


tag = 'DECADE_FINALCUT'
iquery = ("select unitname, count(unitname) as ct "
          "from pfw_attempt a, task t where a.id in "
          f"(select PFW_ATTEMPT_ID from PROCTAG where tag='{tag}') "
          "and t.id=a.task_id and t.status=0 and unitname like 'D0%' "
          "group by unitname having count(unitname)>1")
print(iquery)

cur.execute(iquery)
df = pd.DataFrame(cur.fetchall(), columns=['UNITNAME', 'CT'])
print(df.shape)

bigdf = pd.DataFrame(columns=['PFW_ATTEMPT_ID', 'BAND', 'REQNUM', 'ATTNUM'])
cls = ['NDETS_REF', 'NDETS_REF_HIGHSN', 'CORR_REF', 'CORR_REF_HIGHSN',
       'CHI2_REF', 'CHI2_REF_HIGHSN', 'SIGMA_REF_1',
       'SIGMA_REF_2', 'SIGMA_REF_HIGHSN_1', 'SIGMA_REF_HIGHSN_2',
       'OFFSET_REF_1', 'OFFSET_REF_2',
       'OFFSET_REF_HIGHSN_1', 'OFFSET_REF_HIGHSN_2'
       ]

for unitname in df['UNITNAME']:
    expnum = int(unitname.split('D')[1])
    query = ("select a.ID, e.band, a.reqnum, a.attnum from "
             "PFW_ATTEMPT a, task t, exposure e, proctag p "
             f"where unitname='{unitname}' and e.expnum={expnum} "
             "and t.id=a.task_id and t.status=0 and p.pfw_attempt_id=a.id "
             "and p.tag='DECADE_FINALCUT' and unitname like 'D0%'")
    cur.execute(query)
    out = pd.DataFrame(cur.fetchall(),
                       columns=['PFW_ATTEMPT_ID', 'BAND', 'REQNUM', 'ATTNUM'])
    out['EXPNUM'] = expnum
    print(out)

    for i, row in out.iterrows():
        xml = (f"{unitname}_{row['BAND']}_r{row['REQNUM']}"
               f"p{row['ATTNUM'].zfill(2)}_scamp.xml")

        query = ("select ASTROMNDETS_REF, ASTROMNDETS_REF_HIGHSN, "
                 "ASTROMCORR_REF,ASTROMCORR_REF_HIGHSN, ASTROMCHI2_REF, "
                 "ASTROMCHI2_REF_HIGHSN, "
                 "ASTROMSIGMA_REF_1, ASTROMSIGMA_REF_2, "
                 "ASTROMSIGMA_REF_HIGHSN_1, ASTROMSIGMA_REF_HIGHSN_2, "
                 "ASTROMOFFSET_REF_1, ASTROMOFFSET_REF_2, "
                 "ASTROMOFFSET_REF_HIGHSN_1, ASTROMOFFSET_REF_HIGHSN_2 "
                 "from scamp_qa where filename='{xml}'")
        cur.execute(query)
        res = cur.fetchall()
        out.loc[out['PFW_ATTEMPT_ID'] == row['PFW_ATTEMPT_ID'], cls] = res[0]

    out['SIGMA_REF_HIGHSN'] = sqsum(out['SIGMA_REF_HIGHSN_1'],
                                    out['SIGMA_REF_HIGHSN_2'])
    out['SIGMA_REF'] = sqsum(out['SIGMA_REF_1'], out['SIGMA_REF_2'])
    out['OFFSET_REF_HIGHSN'] = sqsum(out['OFFSET_REF_HIGHSN_1'],
                                     out['OFFSET_REF_HIGHSN_2'])
    out['OFFSET_REF'] = sqsum(out['OFFSET_REF_1'], out['OFFSET_REF_2'])

    # Remove epochs with terrible astrometry (just for doubled entries)
    out = remove_bad(out)
    if out.shape[0] < 2:
        continue

    # Properly tag duplicated entries
    out = remove_duplicates(out)
    if out.shape[0] < 2:
        continue

    chi2_argmin = out['CHI2_REF'].argmin()
    det_argmax = out['NDETS_REF'].argmax()
    chi2_argmax = out['CHI2_REF'].argmax()
    sigmaref_argmin = out['SIGMA_REF'].argmin()

    if chi2_argmin == sigmaref_argmin:
        best = chi2_argmin
    else:
        best = det_argmax

    for i, item in out.iterrows():
        if i == best:
            pass
        else:
            query = ("update PROCTAG set tag='FINALCUT_DUPLICATE'"
                     f"where pfw_attempt_id={item['PFW_ATTEMPT_ID']}"
                     f"and tag='{tag}'")
            cur.execute(query)
            dbh.commit()
