import numpy as np
import json
import argparse
import tqdm


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--numpy", "-n", dest="npfile", required=True)
    parser.add_argument("--json", "-j", dest="jsonfile", required=True)
    return parser


def main(argv):
    dictionary = np.load(argv.npfile, allow_pickle=True)[()]

    dictionary["ptNodes"] = []

    for i in tqdm.tqdm(range(len(dictionary["pt_mask"]))):
        if dictionary["pt_mask"][i]:
            dictionary["ptNodes"].append(i)

    del dictionary["pt_mask"], dictionary["pt_edges"]

    with open(argv.jsonfile, 'w') as f:
      json.dump(dictionary, f)


if __name__ == "__main__":
    argv, _ = build_parser().parse_known_args()
    main(argv)
