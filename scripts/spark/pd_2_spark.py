# Save file local folder, delimiter by default is ,
df.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/tangr/output.csv')

# Save file to HDFS
df.write.format('csv').option('header',True).mode('overwrite').option('sep','|').save('/output.csv')