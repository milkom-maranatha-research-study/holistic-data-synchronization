FROM python:3.7

ENV PYTHONUNBUFFERED 1

# Set working directory
WORKDIR /holistic-data-sync

# Add source code to the working directory
ADD . /holistic-data-sync

# Install all requirements 
RUN pip install -r requirements.txt

# Set Python PATH
ENV PYTHONPATH "${PYTHONPATH}:/holistic-data-sync/sync/"