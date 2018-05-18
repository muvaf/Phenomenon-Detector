file = open("second_version_output.txt", "r+")
lines = []
for line in file:
    lines.append(line.strip())
del lines[0]

newlines = []
for line in lines:
    pieces = line.split("=")
    aline = "{ sets: " + pieces[0] + ", size: " + pieces[1] + "},"
    newlines.append(aline)

out = open("second_version_output_js.txt", "w")
newlines[0] = "[ " + newlines[0]
newlines[-1] = newlines[-1][:-1]
newlines[-1] = newlines[-1] + " ];"
for line in newlines:
    out.write(line)
    out.write("\n")
out.close()
