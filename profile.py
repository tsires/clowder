#!/usr/bin/env python3
if __name__ == '__main__':
    import yappi
    import distfs

    yappi.start()
    distfs.main(['distfs.py', 'fs3', '/tmp/mnt'])
    yappi.stop()

    print('% 8s % 8s % 8s % 8s %s'%('calls','ttot','tsub','tavg', 'name'))
    for f in yappi.get_stats(sort_type=yappi.SORTTYPE_TTOT).func_stats:
        print('% 8d %8.5f %8.5f %8.5f %s'%(f[1],f[2],f[3],f[4],f[0]))

    print('% 8s % 8s % 8s % 8s %s'%('calls','ttot','tsub','tavg', 'name'))
    for f in yappi.get_stats(sort_type=yappi.SORTTYPE_TSUB).func_stats:
        print('% 8d %8.5f %8.5f %8.5f %s'%(f[1],f[2],f[3],f[4],f[0]))

# vim: sw=4 ts=4 expandtab
