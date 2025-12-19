# Use Microsoft OpenJDK 25 on Ubuntu
FROM mcr.microsoft.com/openjdk/jdk:25-ubuntu

# Install Git and wget
RUN apt-get update && \
    apt-get install -y git wget && \
    rm -rf /var/lib/apt/lists/*

# Install Maven 3.9.12 manually
RUN wget https://dlcdn.apache.org/maven/maven-3/3.9.12/binaries/apache-maven-3.9.12-bin.tar.gz -O /tmp/maven.tar.gz && \
    tar -xzf /tmp/maven.tar.gz -C /opt && \
    ln -s /opt/apache-maven-3.9.12 /opt/maven && \
    ln -s /opt/maven/bin/mvn /usr/bin/mvn && \
    rm /tmp/maven.tar.gz

# Configure Maven Toolchains to recognize JDK 25
RUN mkdir -p /root/.m2 && \
    echo '<?xml version="1.0" encoding="UTF-8"?>' > /root/.m2/toolchains.xml && \
    echo '<toolchains>' >> /root/.m2/toolchains.xml && \
    echo '  <toolchain>' >> /root/.m2/toolchains.xml && \
    echo '    <type>jdk</type>' >> /root/.m2/toolchains.xml && \
    echo '    <provides>' >> /root/.m2/toolchains.xml && \
    echo '      <version>25</version>' >> /root/.m2/toolchains.xml && \
    echo '    </provides>' >> /root/.m2/toolchains.xml && \
    echo '    <configuration>' >> /root/.m2/toolchains.xml && \
    echo "      <jdkHome>$JAVA_HOME</jdkHome>" >> /root/.m2/toolchains.xml && \
    echo '    </configuration>' >> /root/.m2/toolchains.xml && \
    echo '  </toolchain>' >> /root/.m2/toolchains.xml && \
    echo '</toolchains>' >> /root/.m2/toolchains.xml

# Set working directory
WORKDIR /usr/src/app

# Clone the repository
RUN git clone https://github.com/apache/datasketches-java.git .

# Create a volume point for output
VOLUME /output

# Run Maven profile to generate files and copy them to the output volume
CMD ["/bin/bash", "-c", "mvn test -P generate-java-files && cp -v serialization_test_data/java_generated_files/*.sk /output/"]
