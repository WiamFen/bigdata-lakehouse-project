# =====================================================
# Hive Metastore custom — compatible Hive 3.1.3
# =====================================================

FROM apache/hive:3.1.3

USER root

# Installer wget
RUN apt-get update && apt-get install -y wget

# =====================================================
# Versions compatibles Hadoop embarqué dans Hive 3.1.3
# IMPORTANT: ne PAS utiliser Hadoop 3.3.x
# =====================================================

RUN wget -O /opt/hive/lib/hadoop-aws-3.1.0.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar \
 && wget -O /opt/hive/lib/aws-java-sdk-bundle-1.11.375.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar \
 && echo "S3A jars installed successfully" \
 && ls -lh /opt/hive/lib/hadoop-aws-3.1.0.jar \
 && ls -lh /opt/hive/lib/aws-java-sdk-bundle-1.11.375.jar

USER hive