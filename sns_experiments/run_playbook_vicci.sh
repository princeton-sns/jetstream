#!
ansible-playbook  -i ansible_hosts_vicci -u princeton_jetstream --module-path=modules/ -f 100 $@; date
