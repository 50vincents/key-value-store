FROM archlinux

RUN pacman -Syu --noconfirm && pacman -S python-pip --noconfirm && pip install flask
COPY . /Asg4
CMD ["python", "/Asg4/assignment4.py"]