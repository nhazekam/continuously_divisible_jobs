from UpRootEventFile import UpRootEventFile
import dimuon

muon_cols   = ['nMuon', 'Muon_pt', 'Muon_eta', 'Muon_phi', 'Muon_mass', 'Muon_charge', 'Muon_mediumId']


f = UpRootEventFile('DYJetsToLL_M-50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_ext1-v2.root')

some = f.events_at(muon_cols, 0,None)

for row in some:
    res = dimuon.dimuonCandidate(*row)
    if res['pass']:
        print(res)

