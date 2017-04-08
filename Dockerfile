FROM python

MAINTAINER Mohit Mittal <mittal.mo@husky.neu.edu>
WORKDIR /src

RUN apt-get update && \
    apt-get clean && \
            rm -rf /var/lib/apt/lists/*

USER root
	        
		# Install Python 3 packages
		# Remove pyqt and qt pulled in for matplotlib since we're only ever going to
		# use notebook-friendly backends in these images
RUN pip install 'pandas' \
		'numexpr' \
		'matplotlib' \
		'scipy' \
		'scikit-learn' \
		'beautifulsoup4' \
		'lxml' \
		'html5lib' \
		'boto' \
		'luigi' \
		'mechanicalsoup'  

									

WORKDIR /src
COPY . /src
EXPOSE 8123		
CMD ["bash"]