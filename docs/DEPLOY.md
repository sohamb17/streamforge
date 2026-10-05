# Deploying the live demo

Two public pieces:

1. **GitHub Pages** (free, always on): the dashboard with the in-browser
   simulation. Built by `.github/workflows/pages.yml` on every push to `main`.
2. **A small VM** (optional): the full stack, so visitors see the real
   cluster and can inject real faults. The Pages site connects to it
   automatically when the `LIVE_URL` repository variable is set, and falls
   back to the simulation when the VM is off.

## 1. GitHub Pages

1. Repository **Settings → Pages → Build and deployment → Source: GitHub Actions**.
2. Push to `main` (or run the `pages` workflow by hand from the Actions tab).
3. The site appears at `https://<user>.github.io/streamforge/`.

## 2. The VM

The stack needs about 2 GB of RAM and 2 vCPUs (Kafka, five store nodes,
two workers, Postgres, Redis, Prometheus, Grafana, Caddy).

### Option A: Oracle Cloud Always Free (Ampere ARM, $0)

As of mid-2026 the Always Free Ampere allowance is 2 OCPU / 12 GB. A card is
needed at sign-up, and popular regions often report "Out of capacity" for
Ampere shapes; retry later or pick another home region at sign-up.

1. Create a VM: Image **Ubuntu 24.04 (aarch64)**, Shape **VM.Standard.A1.Flex**, 2 OCPU, 12 GB.
   Add your SSH public key.
2. Open ports **80 and 443**, in two places:
   - VCN → Subnet → Security List → Ingress rules: TCP 80 and 443 from `0.0.0.0/0`.
   - On the VM itself (Oracle's Ubuntu image ships with restrictive iptables):
     ```bash
     sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 80 -j ACCEPT
     sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 443 -j ACCEPT
     sudo netfilter-persistent save
     ```

### Option B: Hetzner Cloud CX23 (x86, about $6.50 a month)

Create a CX23 with Ubuntu 24.04 and your SSH key. In **Firewalls**, allow
inbound TCP 22, 80 and 443.

### Then, on either VM

```bash
# Docker
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER && newgrp docker

# Code and data
git clone https://github.com/sohamb17/streamforge.git && cd streamforge
data/fetch.sh

# A free hostname that resolves to the VM's public IP, e.g. 203.0.113.7:
export DOMAIN=203-0-113-7.sslip.io
export GRAFANA_ADMIN_PASSWORD='choose-a-long-password'

docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/docker-compose.prod.yml up -d --build
```

Caddy obtains a Let's Encrypt certificate for `$DOMAIN` on first start.
Open `https://$DOMAIN/` for the dashboard and `https://$DOMAIN/grafana/` for
Grafana (anonymous, read-only).

Finally, in the GitHub repository: **Settings → Secrets and variables →
Actions → Variables → New repository variable** `LIVE_URL =
https://203-0-113-7.sslip.io/`, then re-run the `pages` workflow. The Pages
site now shows the live cluster first.

### Operating it

- The demo replayer loops the March 2026 month at 60x forever; `-resume`
  keeps event time increasing across restarts. Kafka keeps 72 hours.
- Visitors' faults are rate limited (one at a time, 6 per minute per
  visitor) and heal themselves within 20 s. Set `CHAOS=off` before
  `docker compose up` to disable them.
- Updating: `git pull && docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/docker-compose.prod.yml up -d --build`.
- Resetting all state: `... down -v`, then `up -d --build`.
- Turning it off to save money: `... down` (keeps volumes) or delete the VM.
  The Pages site keeps working in simulation mode.
