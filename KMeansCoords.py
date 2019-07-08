import sys
reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF-8')
from pyspark import *

''' Given a (latitude/longitude) point and an array of current center points,
    returns the index in the array of the center closest to the given point   '''
def closestPoint(point, center_points):
	nearest_val = float("+inf")
	nearest_index = 0
	for i in range(len(center_points)):
		distance = distanceSquared(point, center_points[i])
		if distance < nearest_val:
			nearest_index = i
			nearest_val = distance
		return nearest_index


''' Given two points, return a point which is the sum of the two points –that is, (x1+x2, y1+y2) '''
def addPoints(point_1, point_2):
	return [point_1[0] + point_2[0], point_1[1] + point_2[1]]


''' Given two points, returns the squared distance of the two.
    This is a common calculation required in graph analysis.   '''
def distanceSquared(point_1, point_2):
	return (point_1[0] - point_2[0]) ** 2 + (point_1[1] - point_2[1]) ** 2


sc = SparkContext()
''' Set the variable K (the number of means to calculate). '''
K = 5

''' Set the variable convergeDist to decide when the k-­‐means calculation is done '''
convergeDist = 0.1

''' Parse the input file, which is delimited by the character ‘,’, into (latitude,longitude) pairs '''
f_name = sys.argv[1]
pts = sc.textFile(f_name).map(lambda line: line.split(",")).map(lambda fields: [float(fields[3]),float(fields[4])]).filter(lambda point: sum(point) != 0).persist()

''' Create a K-­‐length array called kPoints by taking a random sample of K location points from the RDD as starting means (center points). '''
kPoints = pts.takeSample(False, K, 42)


''' Iteratively calculate a new set of K means until the total distance between the means
    calculated for this iteration and the last is smaller than convergeDist               '''
t_distance = float("+inf")
while t_distance > convergeDist:
	# For each coordinate, map each point to a KPoints array index that it's closest to
	closest = pts.map(lambda p : (closestPoint(p, kPoints), (p, 1)))
	# Reduce the result by summing the latitudes and longitudes of points closes to the current center
	reduced = closest.reduceByKey(lambda (point_1, n1), (point_2, n2): (addPoints(point_1, point_2), n1 + n2))
	# Map reduced K members to new center points by calculating the average latitude and longitude for each set of closest points
	newPts = reduced.map(lambda (x, (point, n)): (x, [point[0]/n, point[1]/n])).collect()
	# Calculate how much each center “moved” between the current iteration and the last
	t_distance = 0
	for (x, pt) in newPts:
		t_distance += distanceSquared(kPoints[x], pt)
    	print "distance: ", t_distance
	# Copy the new center points to the kPoints array in preparation for the next iteration
	for (x, pt) in newPts:
		kPoints[x] = pt


''' When the iteration is complete, display the final K center points '''
print "final K center points: " + str(kPoints)

