import paramiko
import getpass

nbytes = 4096
hostname = ["fa18-cs425-g63-01.cs.illinois.edu",
            "fa18-cs425-g63-02.cs.illinois.edu",
            "fa18-cs425-g63-03.cs.illinois.edu",
            "fa18-cs425-g63-04.cs.illinois.edu",
            "fa18-cs425-g63-05.cs.illinois.edu",
            "fa18-cs425-g63-06.cs.illinois.edu",
            "fa18-cs425-g63-07.cs.illinois.edu",
            "fa18-cs425-g63-08.cs.illinois.edu",
            "fa18-cs425-g63-09.cs.illinois.edu",
            "fa18-cs425-g63-10.cs.illinois.edu"]

port = 22
username = getpass.getpass('Username:')
password = getpass.getpass('Password:')
command = 'cd mp4-ys26-weilinz2/ && ./setup.sh'

for i in range(len(hostname)):
    client = paramiko.Transport((hostname[i], port))
    client.connect(username=username, password=password)

    stdout_data = []
    stderr_data = []
    session = client.open_channel(kind='session')
    session.exec_command(command)
    # write and flush?
    while True:
        if session.recv_ready():
            stdout_data.append(session.recv(nbytes))
        if session.recv_stderr_ready():
            stderr_data.append(session.recv_stderr(nbytes))
        if session.exit_status_ready():
            break

    print('exit status: ', session.recv_exit_status())
    print(stdout_data)
    print(stderr_data)


    session.close()
    client.close()
