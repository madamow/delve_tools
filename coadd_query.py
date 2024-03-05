import easyaccess as ea
import pandas as pd
import argparse
from astropy.coordinates import SkyCoord




def get_tiles(args):
    # Read list of tiles from database
    # If window, or specific tile is defined - limit the list

    print("Reading coaddtiles_geom")

    iter = 0
    tcolumn = ['tilename', 'crossra0', 'RACMIN', 'RACMAX', 'DECCMIN', 'DECCMAX']
    tile = pd.DataFrame(columns=tcolumn)
        
    q0 = "select tilename, crossra0, RACMIN, RACMAX, DECCMIN, DECCMAX from coaddtile_geom "
    q0+= "where id<200000 "


    if args.tile is not None:
        q1 = q0 + "and tilename='%s'" % args.tile
    elif args.window is not None:
        try:
            ramin, ramax,decmin, decmax = args.window.split(",")
        except ValueError:
            print("Four number needed: RAmin, RAmax, DECmin, DECmax")
            print ("%i provided" % len(args.window.split(",")))
            exit()
        if float(ramin) < float(ramax):
            q1 = q0 + " and RACMIN>%s and RACMAX<%s and DECCMIN>%s and DECCMAX<%s and crossra0='N' " % (ramin, ramax, decmin, decmax)
        else:
            q1 = q0 + " and ((RACMIN>%s and RACMIN<360) or (RACMIN>0 and RACMIN<%s))" % (ramin,ramax) 
            q1+= " and ((%s<RACMAX and RACMAX<360) or (0<RACMAX and RACMAX<%s))" % (ramin,ramax) 
            q1+= " and DECCMIN>%s and DECCMAX<%s " % (decmin, decmax)
    else:
         q1 = q0
    
    
    tiles = con.query_results(q1)
    tdf = pd.DataFrame(tiles, columns=tcolumn)
    tdf['l1'] = None
    tdf['b1'] = None
    tdf['l2'] = None
    tdf['b2'] = None
    tdf['l3'] = None
    tdf['b3'] = None
    tdf['l4'] = None
    tdf['b4'] = None

    # Add galactic coordinates
    for i, row in tdf.iterrows():
        c = SkyCoord(row['RACMIN'], row['DECCMIN'], frame='icrs', unit='deg')
        lb = c.galactic
        tdf.loc[i, ['l1']] = lb.l.deg
        tdf.loc[i, ['b1']] = lb.b.deg

        c = SkyCoord(row['RACMIN'], row['DECCMAX'], frame='icrs', unit='deg')
        lb = c.galactic
        tdf.loc[i, ['l2']] = lb.l.deg
        tdf.loc[i, ['b2']] = lb.b.deg

        c = SkyCoord(row['RACMAX'], row['DECCMIN'], frame='icrs', unit='deg')
        lb = c.galactic
        tdf.loc[i, ['l3']] = lb.l.deg
        tdf.loc[i, ['b3']] = lb.b.deg

        c = SkyCoord(row['RACMAX'], row['DECCMAX'], frame='icrs', unit='deg')
        lb = c.galactic
        tdf.loc[i, ['l4']] = lb.l.deg
        tdf.loc[i, ['b4']] = lb.b.deg

    tdf.to_csv('Antila2_tiles_geometry.csv')

    return tdf

def get_tiles_for_propid(args):
    print("\n Working on tables: exposure, image\n")
    
    tiles = get_tiles(args)
    
    tiles_propid = []
    
    q0 = "select i.expnum, i.filename, i.RACMIN, i.RACMAX, i.DECCMIN, i.DECCMAX, i.band "
    q0 += "from image i, exposure e, task t, pfw_attempt a "
    q0 += " where e.propid='%s' " % args.propid
    q0 += "and e.obstype='object' and e.expnum=i.expnum and i.filetype='red_immask' "
    q0 += "and a.id=i.pfw_attempt_id and a.task_id=t.id and t.status=0 "
    q0 += "and exists (select 1 from finalcut_eval f where i.pfw_attempt_id =f.pfw_attempt_id and f.accepted='True') "
    #q0 += "and exists (select 1 from zeropoint z where z.imagename=i.filename and z.flag<16) "
    
    r1 = con.query_results(q0)
    df = pd.DataFrame(r1, columns=['expnum', 'filename', 'RACMIN', 'RACMAX', 'DECCMIN', 'DECCMAX', 'band'])
    dfs = df.shape[0]
  
    for i, row in df.iterrows():
        if i % 1000 == 0:
            print("%i / %i  %4.2f / 100 " % (i, dfs, i*100./dfs))
        
        if row['RACMIN']<row['RACMAX']:
            tile_name = tiles[(
                      (((tiles['RACMIN']<row['RACMIN']) & (row['RACMIN']<tiles['RACMAX'])) |
                       ((tiles['RACMIN']<row['RACMAX']) & (row['RACMAX']<tiles['RACMAX']))) &
                      (((tiles['DECCMIN']<row['DECCMIN']) & (row['DECCMIN']<tiles['DECCMAX'])) | ((tiles['DECCMIN']<row['DECCMAX']) & (row['DECCMIN']<tiles['DECCMAX'])))
                      )]
        else:
            tile_name = tiles[(
                         (((tiles['RACMIN']<row['RACMIN']) & (row['RACMIN']<360.)) | ((0<row['RACMIN']) & (row['RACMIN']<tiles['RACMAX'])) |
                          ((tiles['RACMIN']<row['RACMAX']) & (row['RACMAX']<360.)) | ((0<row['RACMAX']) & (row['RACMAX']<tiles['RACMAX']))) &
                         (((tiles['DECCMIN']<row['DECCMIN']) & (row['DECCMIN']<tiles['DECCMAX'])) | ((tiles['DECCMIN']<row['DECCMAX']) & (row['DECCMIN']<tiles['DECCMAX'])))
                         )]
            
        for tname in tile_name.tilename.values:
            if tname not in tiles_propid:
                tiles_propid.append(tname)
    pid = pd.DataFrame(tiles_propid, columns=['tilename'])
    pid_tiles = pid.merge(tiles, on=['tilename']) 
    return pid_tiles

def get_images(args):
    print("\n Working on tables: image, proctag, zeropoint")
    
    column = ['BAND', 'FILENAME', 'RACMIN','RACMAX','DECCMIN', 'DECCMAX']
    
    df = pd.DataFrame(columns=column)
    # This query is contructed under an assumption that all exposures tagged for coadd production
    # ende with status = 0 in task table. If proctag is defined, task and pfw_attempt tables will not be queried
    #q0 = "select i.band, i.filename, i.RACMIN, i.RACMAX, i.DECCMIN, i.DECCMAX "

    #q0 += "from image i, task t, pfw_attempt a, zeropoint z "
    #q0 += "from image i "
    #q0 += "where i.filetype='red_immask' "
    #q0 += "and a.id=i.pfw_attempt_id and a.task_id=t.id and t.status=0 "
    #q0 += "and i.expnum in (select distinct(expnum) from DECADE_ZPS_NGC55_20230209) "
    #q0 += "and exists (select 1 from DECADE_REFCAT2_13_0 z where z.imagename=i.filename) "
    q0 = "select i.band, i.filename, i.RACMIN, i.RACMAX, i.DECCMIN, i.DECCMAX from image i, proctag p where "
   # q0+= "exposure e where i.expnum=e.expnum and e.obstype='object' and e.object not like 'pointing' and e.object not like 'Sgr-%' "
    q0+= " i.pfw_attempt_id=p.pfw_attempt_id and p.tag='DECADE_FINALCUT' and i.filetype='red_immask'"
    #DECADE_ZPS_NGC55_20230209 z "
    #q0 += "where i.filetype='red_immask' and i.filename=z.imagename"
  #  if args.proctag:
  #      q0 += "and i.pfw_attempt_id in (select pfw_attempt_id from proctag p where p.tag='%s') " % args.proctag
    #q0 += "and exists (select 1 from finalcut_eval f where i.pfw_attempt_id =f.pfw_attempt_id and f.accepted='True') "
    print(q0)
    out = con.query_results(q0)
    r1 = pd.DataFrame(out, columns=column)
    with open(args.output+"_images.csv", 'a') as f:
        r1.to_csv(f, index=False)
    return r1


def get_tiles_for_images(args):
#    import math
    #try:
        #df = pd.read_csv(args.output+"_images.csv")
    df = pd.read_csv("dr32_mis_images.csv")
    #df['DECCMIN'] = pd.to_numeric(df['DECCMIN'])
    #df['DECCMAX'] = pd.to_numeric(df['DECCMAX'])
    # dmin=df['DECCMIN'].min()
    #print(dmin)
#    dmax=df['DECCMAX'].max()

    tiles = pd.read_csv('tiles_geometry.csv')

  #  tiles = tiles[tiles['DECCMIN']>=dmin]
  #  tiles = tiles[tiles['DECCMAX']<=dmax]
   #tiles=tiles.reset_index()



        #df = pd.read_csv("test.csv")
    print("Found existing file")
    #except:
    #    df = get_images(args)
    dfinal = pd.DataFrame()
#
#    demin = math.floor(df['DECCMIN'].min())
#    demax = math.ceil(df['DECCMAX'].max())
#
#    if all(df['RACMIN']<df['RACMAX']):
#        ramin = math.floor(df['RACMIN'].min())
#        ramax = math.ceil(df['RACMAX'].max())
#    else:
#        ramin_vals = list(df['RACMIN'].sort_values())
#        ramax_vals = list(df['RACMAX'].sort_values())
#
#        for i in range(1,len(ramin_vals)):
#            if ramin_vals[i]-ramin_vals[i-1]>1.:
#                ramin = math.floor(ramin_vals[i])
#                break
#        for i in range(1,len(ramax_vals)):
#            if ramax_vals[i]-ramax_vals[i-1]>1.:
#                ramax =math.ceil(ramax_vals[i-1])


#    args.window = ",".join([str(ramin),str(ramax),str(demin),str(demax)])
#    print(args.window)

#    tiles = get_tiles(args)
   # tiles = pd.read_csv('dr3_r1_2_tiles.csv')
    for i, tile in tiles.iterrows():
        print("%2i / %i" % (i+1, tiles.shape[0]), tile['tilename'])

        if tile.crossra0[0] == 'N':

            df_in = df[(((df['RACMIN'].between(tile.RACMIN, tile.RACMAX)) | (df['RACMAX'].between(tile.RACMIN, tile.RACMAX))) &
                        ((df['DECCMIN'].between(tile.DECCMIN, tile.DECCMAX)) | (df['DECCMAX'].between(tile.DECCMIN, tile.DECCMAX))))]


        else:
            df_in = df[(((df['RACMIN'].between(tile.RACMIN, 360.)) | (df['RACMAX'].between(tile.RACMIN, 360.)) |
                         (df['RACMIN'].between(0., tile.RACMAX)) | (df['RACMAX'].between(0., tile.RACMAX))) &
                        ((df['DECCMIN'].between(tile.DECCMIN, tile.DECCMAX)) | (df['DECCMAX'].between(tile.DECCMIN, tile.DECCMAX))))]

        df_in['TILENAME'] = tile['tilename']
        dfinal = dfinal.append(df_in)


    dfinal = dfinal.drop_duplicates()
    dfinal = dfinal.reset_index(drop=True)

    print("Saving to file")
    dfinal.to_csv(args.output+"_image_to_tile.csv", index=False)


    print("\t ...done")
    return dfinal

def get_tiles_for_expnums(expnums, args, proctag='DECADE_FINALCUT'):
    print("\n Working on tables: image, proctag, zeropoint")

    column = ['band', 'FILENAME', 'TILENAME']

    df = pd.DataFrame(columns=column)

    q0 = "select i.band, i.filename "
    q0 += "from image i, task t, pfw_attempt a, proctag p "
    q0 += "where i.filetype='red_immask' "
    q0 += "and a.id=i.pfw_attempt_id and a.task_id=t.id and t.status=0 "
    q0 += "and a.id=p.pfw_attempt_id and p.tag= %s " % (proctag)

    for i, tile in tiles.iterrows():
        print("%2i / %i" % (i+1, tiles.shape[0]), tile)
        if tile.crossra0[0] == 'N':
            q1 = q0 + "and (i.RACMIN between %f and %f or i.RACMAX between %f and %f) and " % (tile.RACMIN, tile.RACMAX, tile.RACMIN, tile.RACMAX)
            q1+= "(i.DECCMIN between %f and %f or i.DECCMAX between %f and %f) " % (tile.DECCMIN, tile.DECCMAX, tile.DECCMIN, tile.DECCMAX)
        else:
            q1 = q0 + "and (i.RACMIN between %f and 360 or i.RACMIN between 0 and %f or " % (tile.RACMIN, tile.RACMAX)
            q1 += " i.RACMAX between %f and 360 or i.RACMAX between 0 and %f ) and " % (tile.RACMIN, tile.RACMAX)
            q1 += "(i.DECCMIN between %f and %f or i.DECCMAX between %f and %f) " % (tile.DECCMIN, tile.DECCMAX, tile.DECCMIN, tile.DECCMAX)
        out = con.query_results(q1)
        try:
            print("Saving to file")
            r1 = pd.DataFrame(out, columns=['band', 'FILENAME'])
            r1['TILENAME'] = tile['tilename']

            with open(args.output, 'a') as f:
                r1.to_csv(f, header=False, index=False)
        except:
            print("Failed to save to file")
            pass
  #      df = df.append(r1)


   # df = df.drop_duplicates()
   # print "\t ...done"
   # return df


def tag_coadds():
     query = "select distinct(c.pfw_attempt_id), c.tilename, a.attnum "
     query += "from coadd c, pfw_attempt a "
     query += "where a.reqnum=5230 and c.filetype='coadd' "
     query += "and c.pfw_attempt_id = a.id"
     out = con.query_results(query)
     df = pd.DataFrame(out, columns=['pfw_attempt_id','tilename', 'attnum'])
     df['att']=0
     df_max = pd.DataFrame()
     for tile, grp in df.groupby(by='tilename'):
         mx = grp.loc[grp['attnum'].idxmax()]
         df_max = df_max.append(mx, ignore_index=True)

     df_max.to_csv('2020A_0238_coadds.csv')


def insert_to_proctag(df):
    from despydb import DesDbi
    dbh = DesDbi(None, 'db-decade', retry=True)
    cur = dbh.cursor()
    tag_coadds()

    me = 'madamow_decade'
    for i, row in df.iterrows():
        id = int(row['PFW_ATTEMPT_ID'])
        tag = row['TAG']
        
        insert_query = "insert into proctag (tag,pfw_attempt_id) values ('%s',%i)" % (tag, id)

        cur.execute(insert_query)
    dbh.commit()


def insert_to_image_tile(args, tbl="DECADE_IMAGE_TO_TILE"):

    from despydb import DesDbi

    dbh = DesDbi(None, 'db-decade', retry=True)
    cur = dbh.cursor()

    df = pd.read_csv(args.output)
    for i, row in df.iterrows():
       # checkq = "select filename from %s where filename='%s' and tilename='%s'" % (tbl, row['FILENAME'],row['TILENAME'])
       # out = con.query_results(checkq)
        #print(i, "inserting", row['FILENAME'])

        query = "insert into %s (filename,tilename) values ('%s', '%s')" % (tbl, row['FILENAME'],row['TILENAME'])
        cur.execute(query)
        dbh.commit()


def refcat_exps(tiles,args):
    #This is only to get info on SexB and NGC55 deep exposures with refcat2 zeropoints
    print("\n Working on tables: image, proctag, zeropoint")

    column = ['band', 'FILENAME', 'TILENAME']

    df = pd.DataFrame(columns=column)

    q0 = "select i.band, i.filename,z.tag "
    q0 += "from image i, zeropoint z "
    q0 += "where i.filename=z.imagename "
    q0 += "and z.tag='ngc55'  "

    for i, tile in tiles.iterrows():
        print("%2i / %i" % (i+1, tiles.shape[0]), tile)
        if tile.crossra0[0] == 'N':
            q1 = q0 + "and (i.RACMIN between %f and %f or i.RACMAX between %f and %f) and " % (tile.RACMIN, tile.RACMAX, tile.RACMIN, tile.RACMAX)
            q1+= "(i.DECCMIN between %f and %f or i.DECCMAX between %f and %f) " % (tile.DECCMIN, tile.DECCMAX, tile.DECCMIN, tile.DECCMAX)
        else:
            q1 = q0 + "and (i.RACMIN between %f and 360 or i.RACMIN between 0 and %f or " % (tile.RACMIN, tile.RACMAX)
            q1 += " i.RACMAX between %f and 360 or i.RACMAX between 0 and %f ) and " % (tile.RACMIN, tile.RACMAX)
            q1 += "(i.DECCMIN between %f and %f or i.DECCMAX between %f and %f) " % (tile.DECCMIN, tile.DECCMAX, tile.DECCMIN, tile.DECCMAX)

        out = con.query_results(q1)
        try:
            print("Saving to file")
            r1 = pd.DataFrame(out, columns=['band', 'FILENAME','tag'])
            r1['TILENAME'] = tile['tilename']

            with open(args.output, 'a') as f:
                r1.to_csv(f, index=False)
        except:
            print("Failed to save to file")

            pass

def find_pfw_attempt_for_exposures(tag=None, file=None):
    df = pd.read_csv(file)
    unitnames = ['D' + str(e).zfill(8) for e in df['EXPNUM'].values]
    i = 0
    query_db = True
    out_df = pd.DataFrame(columns=['PFW_ATTEMPT_ID','UNITNAME','STATUS'])
    while query_db:
        explist = unitnames[i*1000:(i+1)*1000]
        q0 = "select a.id, a.unitname, t.status "
        q0 += "from task t, pfw_attempt a, proctag p "
        q0 += "where  a.task_id=t.id and t.status=0 "
        q0 += "and exists (select 1 from finalcut_eval f where a.id =f.pfw_attempt_id and f.accepted='True') "
        q0 += "and a.id=p.pfw_attempt_id and p.tag='DECADE_FINALCUT' "
        q0 += "and unitname in ('%s') " % ("','".join(explist))
        i+=1
        if len(explist)<1000:
            query_db = False
        out = pd.DataFrame(con.query_results(q0), columns=['PFW_ATTEMPT_ID','UNITNAME','STATUS'])
        out_df = out_df.append(out)
    out_df = out_df.reset_index(drop=True)
    out_df['TAG'] = tag

    return out_df



con = ea.connect('decade')

#tag_coadds()
#insert_to_proctag('2020A_0238_coadds.csv', tag='DELVE_CENI')
#exit()
parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--window', help='ramin, ramax, decmin, decmax',type=str, required=False, default=None)
parser.add_argument('--tile', default=None, required=False, help='tile identifier',  type=str)
parser.add_argument('--propid', default=None, required=False, help='proposal id',  type=str)
parser.add_argument('--out', help='name of oot file to store results',  type=str)
parser.add_argument('--images_to_tile', action='store_true', help='insert to decade_image_to_tile')
parser.add_argument('--output', required=True, help='name of the output file with tile-image info')
parser.add_argument('--proctag', help='proctag',  type=str)
args = parser.parse_args()

#to_tag = find_pfw_attempt_for_exposures(tag='NGC55_finalcut', file='ngc55_full_expnum.csv')
#These are three steps that have to be done before submit_multiepoch

#tiles = get_tiles(args)
#exit()
#images =get_images(args)
#print("Done with images")
get_tiles_for_images(args)

#insert_to_image_tile(args, tbl='DELVE_NGC55_IMAGE_TO_TILE')
#insert_to_proctag(to_tag)
