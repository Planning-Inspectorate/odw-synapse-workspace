import subprocess

def extract_jar_files():
    """
    Extract jar files using build.gradle
    """
    build_command = "gradle build -p configuration/workspace-packages"
    subprocess.check_output(build_command.split(" "))
    unpack_command = "gradle extractJarFiles -p configuration/workspace-packages"
    subprocess.check_output(unpack_command.split(" "))