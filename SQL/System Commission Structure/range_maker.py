import pandas as pd


def main():
    dr = []
    for i in dr:
        print(i)

    num = .0001
    dr.append(num)

    while num <= 1:
        num += .0001
        dr.append(num)

    for i in dr:
        print(i)

    data_range = pd.DataFrame(dr)
    data_range.to_csv('./data_range.csv')


if __name__ == '__main__':
    main()
