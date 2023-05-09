import yaml


def get_config(env):
    with open(f"config/{env}.yaml", "r") as yamlfile:
        data = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("Read successful")

        return data
