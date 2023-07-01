import subprocess

def get_qdisc_info(device):
    command = ["tc", "-s", "qdisc", "show", "dev", device]
    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    output, error = process.communicate()

    if error:
        print("Error retrieving qdisc info: {error}")
        return None
    else:
        return output.decode("utf-8")

device = "ens3"
print(get_qdisc_info(device))