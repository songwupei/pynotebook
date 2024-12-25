from collections import defaultdict

import toml

data = toml.load("/home/song/.config/yazi/keymap.toml")
km = data["manager"]["keymap"]

keymap_ddict = defaultdict(list)
for i in km:
    for k, v in i.items():
        print(k, v)
        keymap_ddict[k].append(v)
    break
print(keymap_ddict)
