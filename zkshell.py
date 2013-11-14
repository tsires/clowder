#!/usr/bin/env python3
""" Zookeeper Command Shell

Run zookeeper commands for debugging or maintenance.

For command details, see:
https://zookeeper.apache.org/doc/r3.1.2/zookeeperAdmin.html#sc_zkCommands

"""

from __future__ import with_statement, division, print_function, absolute_import, unicode_literals

import cmd
from kazoo.exceptions import ConnectionLoss
# NOTE: socket.error is Deprecated as of Python 3.3, now aliases OSError builtin
from socket import error as SocketError

class ZKShell(cmd.Cmd):
    """ Zookeeper Command Shell """
    #intro = "Zookeeper Command Shell\nType help or ? to list commands.\n"
    prompt = "zk> "

    def __init__(self, zk, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._zk = zk

    def _error(self, message, subject):
        print("*** %s: %s" % (message, subject))

    def default(self, line):
        if line == "EOF":
            print("exit")
            return self.do_exit("")
        self._error("Unrecognized command", line)

    def _send_command(self, cmd):
        try:
            print(self._zk.command(cmd=cmd))
        except ConnectionLoss as e:
            self._error("Connection not open", e)
        except SocketError as e:
            self._error("Error in underlying socket", e)

    def do_dump(self, arg):
        """Lists the outstanding sessions and ephemeral nodes. This only works on the leader."""
        self._send_command(b"dump")

    def do_envi(self, arg):
        """Print details about serving environment."""
        self._send_command(b"envi")

    def do_kill(self, arg):
        """Shuts down the server. This must be issued from the machine the ZooKeeper server is running on."""
        self._send_command(b"kill")

    def do_reqs(self, arg):
        """List outstanding requests."""
        self._send_command(b"reqs")

    def do_ruok(self, arg):
        """Tests if server is running in a non-error state. The server will respond with imok if it is running. Otherwise it will not respond at all."""
        self._send_command(b"ruok")

    def do_srst(self, arg):
        """Reset statistics returned by stat command."""
        self._send_command(b"srst")

    def do_stat(self, arg):
        """Lists statistics about performance and connected clients."""
        self._send_command(b"stat")

    def do_exit(self, arg):
        """Exit the shell."""
        return True


if __name__ == '__main__':
    from kazoo.client import KazooClient
    # NOTE: argparse is new in Python 3.2
    import argparse

    COMMANDS = ["dump","envi","kill","reqs","ruok","srst","stat"]

    # TODO: accept auth info
    parser = argparse.ArgumentParser(description='Send commands to a Zookeeper server.')
    parser.add_argument('-s', '--server', default='localhost:2181')
    parser.add_argument('cmd', nargs='*', default=['shell'], help='a Zookeeper command or "shell"')

    args = parser.parse_args()

    # Create a Zookeeper client for our shell
    zk = KazooClient(hosts=args.server)
    zk.start()
    sh = ZKShell(zk)

    if args.cmd[0] == 'shell':
        # Run interactively
        sh.cmdloop()
    else:
        # Run one command
        sh.onecmd(" ".join(args.cmd))

    zk.stop()


# vim: sw=4 ts=4 expandtab
