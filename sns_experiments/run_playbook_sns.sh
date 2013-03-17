#!
ansible-playbook  -i ansible_hosts_sns -u arye --module-path=modules/ -f 100 -K $@; date
