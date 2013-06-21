f = File.open("movielen_dataset.csv", "w")
File.open("ratings.dat").each do |l|
	temp = l.split("::")
	userID  = temp[0]
	movieID = temp[1]
	rating  = temp[2]
	f << "#{userID},#{movieID},#{rating}\n"
end
f.close