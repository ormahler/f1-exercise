import yaml


def get_config(env):
    with open(f"python_code/pit-stops-producer/config/{env}.yaml", "r") as yamlfile:
        data = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("Read successful")

        return data
