import pandas as pd
import easyaccess as ea
from argparse import ArgumentParser, RawDescriptionHelpFormatter


def query_database(inargs):
    if inargs.propid:
        query1 = "select expnum from exposure where obstype='object' "
        query1 += f"and propid='{inargs.propid}'"
        out = con.query_results(query1)
        elist = pd.DataFrame(out, columns=['EXPNUM'])
    elif inargs.explist:
        explist = pd.read_csv(inargs.explist)
        elist = explist.astype({'EXPNUM': str})

        query_db = True
        in_db = pd.DataFrame()
        i = 0
        while query_db:
            el1 = elist.EXPNUM[i*1000:(i+1)*1000]
            estr = ",".join(el1)
            q1 = f"select expnum,band from exposure where expnum in ({estr})"
            if len(el1) > 0:
                out = pd.DataFrame(con.query_results(q1),
                                   columns=['EXPNUM', 'BAND'])
            else:
                pass
            in_db = in_db.append(out)
            i += 1
            if i*1000 > elist.shape[0]:
                query_db = False

        elist = elist.astype({'EXPNUM': int})
        df_all = elist.merge(in_db.drop_duplicates(), on=['EXPNUM'],
                             how='left', indicator=True)
        not_in = df_all[df_all['_merge'] == 'left_only']
        not_in.to_csv(args.project+'not_in_database.csv', index=False)
        elist = df_all[df_all['_merge'] == 'both']

    elist['unitname'] = elist['EXPNUM'].apply(lambda x: 'D'+str(x).zfill(8))
    elist['status'] = 'not processed'
    elist['exit_code'] = None
    elist['id'] = None

    # Divide list in 1000 long chunks
    query_db = True
    i = 0
    df_status = pd.DataFrame()
    while query_db:
        el2 = ("','".join(elist.unitname[i*1000:(i+1)*1000]))
        query2 = "select a.unitname, a.id, t.status from pfw_attempt a, "
        query2 += "task t,pfw_request r where r.reqnum=a.reqnum "
        query2 += "and t.id=a.task_id and r.project in ('DEC','DEC_Taiga') "
        query2 += f"and a.unitname in ('{el2}')"

        out = pd.DataFrame(con.query_results(query2),
                           columns=['unitname', 'id', 'status'])
        df_status = df_status.append(out)
        i += 1
        if len(out) == 0:
            query_db = False

    df_status = df_status.fillna(-99)

    for name, dfchunk in df_status.groupby(by=['unitname'], sort=True):
        if 0 in dfchunk.status.values:
            elist.loc[elist['unitname'] == name, 'status'] = 'done'
            elist.loc[elist['unitname'] == name, 'exit_code'] = 0
        elif -99 in dfchunk.status.values:
            elist.loc[elist['unitname'] == name, 'status'] = 'in queue'
        else:
            elist.loc[elist['unitname'] == name, 'status'] = 'failed'
            slist = " ".join([str(int) for int in dfchunk.status.values])
            elist.loc[elist['unitname'] == name, 'exit_code'] = slist
        idlist = " ".join([str(int) for int in dfchunk.id.values])
        elist.loc[elist['unitname'] == name, 'id'] = idlist

    return elist


parser = ArgumentParser(description=__doc__,
                        formatter_class=RawDescriptionHelpFormatter)

parser.add_argument('--expnum', default=None, required=False,
                    help='tile identifier',  type=str)
parser.add_argument('--propid', default=None, required=False,
                    help='proposal id',  type=str)
parser.add_argument('--out', default=None, required=False,
                    help='name of oot file to store results',  type=str)
parser.add_argument('--explist', help='exposure list in text file',  type=str)
parser.add_argument('--project', default='DEC_Taiga', required=False)
args = parser.parse_args()

if args.project == 'DEC' or args.project == 'DEC_Taiga':
    con = ea.connect('decade')
elif args.project == 'OPS':
    con = ea.connect('desoper')
else:
    print("project not defined")
    exit()
df = query_database(args)
print(df)
df.to_csv('out.csv')
