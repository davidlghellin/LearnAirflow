def principal():
    fichero = open("xyz_grande.csv", 'w')
    c = 0
    while c < 4:
        fichero.write("y\n")
        fichero.write("z\n")
        c += 1
    c = 0
    while c < 1000000000: # 2gigas+-
        fichero.write("x\n")
        c += 1
    fichero.close()

    fichero = open("xyz_medio.csv", 'w')
    c = 0
    while c < 1:
        fichero.write("y\n")
        fichero.write("z\n")
        c += 1
    c = 0
    while c < 10000000: # 20megas
        fichero.write("x\n")
        c += 1
    fichero.close()


if __name__ == '__main__':
    principal()
