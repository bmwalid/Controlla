import sys
import os
sys.path.append(os.path.abspath("/home/hadoop/KYLIN_USB/sources/run/*"))

def main():
    # Import Modules
    # Import global variables

    # Import Sparkles class
    import global_variable as gv
    import sparklers as sp
    # instantiate master of sparklers
    master = sp.MasterSparkles(gv.Tfacts, gv.Tsources)
    # Prepare registry for data preparation
    master.data_prep_refresh()
    # declare pool of spark execution (we can have a maximum of ten spark treatment running at the same time)
    pool = sp.executor("Pool", 10)
    # get the registry
    registry = master.registry
    # launch automatic execution
    pool.launch(registry=registry)
    # TODO
    # Change core-site.xml before cubing

    # get cube registry
    # ky.MasterKylin()
    # ky.pool()
    # ky.launch()
    # persist data
    # ps.hdfs_to_s3()


if __name__ == '__main__':
    main()
