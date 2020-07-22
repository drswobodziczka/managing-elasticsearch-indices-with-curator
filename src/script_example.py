#!/usr/bin/python3

import sys, getopt


def main(argv):

  print(sys.argv[1:])

  shape = ''
  color = ''
  try:
    opts, args = getopt.getopt(argv[1:], "", ["help", "shape=", "color="])
  except getopt.GetoptError:
    print('Usage: test.py --shape <box|circle|...> --color <white|red|green|...>')
    sys.exit(2)
  for opt, arg in opts:
    if opt == '--help':
      print('Usage: test.py --shape <box|circle|...> --color <white|red|green|...>')
      sys.exit()
    elif opt == "--shape":
      shape = arg
    elif opt == "--color":
      color = arg
  print('Input shape is ', shape)
  print('Input color is ', color)


if __name__ == "__main__":
  main(sys.argv[1:])
