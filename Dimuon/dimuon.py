from math import *
import sys

output_fields = ['pass', 'mass', 'ptp', 'eta', 'phi', 'dPhi', 'dR', 'dEta', 'mu1_pt', 'mu2_pt', 'mu1_eta', 'mu2_eta', 'mu1_phi', 'mu2_phi']

def deltaPhi(phi1,phi2):
    dphi = (phi1-phi2)
    while dphi >  pi: dphi -= 2*pi
    while dphi < -pi: dphi += 2*pi
    return dphi

def deltaR(eta1,phi1,eta2=None,phi2=None):
    return hypot(eta1-eta2, deltaPhi(phi1,phi2))

def invMass(pt1, pt2, eta1, eta2, phi1, phi2, mass1, mass2):
    theta1 = 2.0*atan(exp(-eta1))
    px1    = pt1 * cos(phi1)
    py1    = pt1 * sin(phi1)
    pz1    = pt1 / tan(theta1)
    E1     = sqrt(px1**2 + py1**2 + pz1**2 + mass1**2)

    theta2 = 2.0*atan(exp(-eta2))
    px2    = pt2 * cos(phi2)
    py2    = pt2 * sin(phi2)
    pz2    = pt2 / tan(theta2)
    E2     = sqrt(px2**2 + py2**2 + pz2**2 + mass2**2)

    themass  = sqrt((E1 + E2)**2 - (px1 + px2)**2 - (py1 + py2)**2 - (pz1 + pz2)**2)
    thept    = sqrt((px1 + px2)**2 + (py1 + py2)**2)
    thetheta = atan( thept / (pz1 + pz2) )
    theeta   = 0.5*log( (sqrt((px1 + px2)**2 + (py1 + py2)**2 + (pz1 + pz2)**2)+(pz1 + pz2))/(sqrt((px1 + px2)**2 + (py1 + py2)**2 + (pz1 + pz2)**2)-(pz1 + pz2)) )
    thephi   = asin((py1 + py2)/thept)

    delPhi = deltaPhi(phi1,phi2)
    delR   = deltaR(eta1,phi1,eta2,phi2)
    delEta = eta1-eta2

    return (themass, thept, theeta, thephi, delPhi, delR, delEta)

def dimuonCandidate_aux(pt, eta, phi, mass, charge, mediumid):
    # default class implementation
    default_ = (False, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    """
    Z->mm candidate from arbitrary muon selection:
      N(mu) >= 2
      pT > 30, 10
      abs(eta) < 2.4, 2.4
      mediumId muon
      opposite charge
    """

    if len(pt) < 2:
        return default_

    #Identify muon candidate
    leadingIdx = None
    trailingIdx = None

    for idx in range(len(pt)):
        if leadingIdx == None:
            if pt[idx] > 30 and abs(eta[idx]) < 2.4 and mediumid[idx]:
                leadingIdx = idx
        elif trailingIdx == None:
            if pt[idx] > 10 and abs(eta[idx]) < 2.4 and mediumid[idx]:
                trailingIdx = idx
        else:
            if pt[idx] > 10 and abs(eta[idx]) < 2.4 and mediumid[idx]:
                return default_

    if leadingIdx != None and trailingIdx != None and charge[leadingIdx] != charge[trailingIdx]:
        # Candidate found
        dimuon_   = (True,) + \
                    invMass(pt[leadingIdx], pt[trailingIdx],
                            eta[leadingIdx], eta[trailingIdx],
                            phi[leadingIdx], phi[trailingIdx],
                            mass[leadingIdx], mass[trailingIdx]) + \
                    (pt[leadingIdx], pt[trailingIdx],
                     eta[leadingIdx], eta[trailingIdx],
                     phi[leadingIdx], phi[trailingIdx])
        return dimuon_
    else:
        return default_

def dimuonCandidate(pt, eta, phi, mass, charge, mediumid):
    return dict(zip(output_fields, dimuonCandidate_aux(pt, eta, phi, mass, charge, mediumid)))

try:
    import matplotlib
    matplotlib.use('agg')
    import matplotlib.pyplot as plt
    import histogrammar as hg
    import json

    import logging
    mpl_logger = logging.getLogger('matplotlib')
    mpl_logger.setLevel(logging.WARNING)

    invmass = hg.Bin(80, 70, 110, quantity=lambda x: x, value=hg.Count())

    def histogram_fill_from_file(filename):
        with open(filename, 'r') as f_in:
            for row in f_in:
                js = json.loads(row)
                invmass.fill(js['mass'])

    def histogram(output='output.png'):
        #ax = invmass.plot.matplotlib(name="", color="green", edgecolor="white", lw=5)
        ax = invmass.plot.matplotlib(name="")
        ax.set_xlabel('Dimuon invariant mass m($\mu\mu$) (GeV)')
        ax.set_ylabel('Events / 0.5 GeV')
        plt.savefig(output)
        print(json.dumps(invmass.toJson()))
        #plt.show()
except ImportError:
    def histogram_fill_from_file(filename):
        sys.stderr.write('Import Error')

    def histogram(output='output.png'):
        sys.stderr.write('Import Error')

