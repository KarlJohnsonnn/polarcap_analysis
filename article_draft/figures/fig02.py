#See LICENSES/ for license information
#SPDX-License-Identifier: BSD-3-Clause

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def resolution(group, element):
    tmp = (2**(group + ((element-1)/6)))**(-1)
    return tmp*1e3


res_path = r'./data/USAF_reolution_target.xlsx'

names = ['img','z','group1','element1','group2','element2']
res = pd.read_excel(res_path,names=names)
res['res1']=resolution(res.group1, res.element1)
res['res2']=resolution(res.group2, res.element2)
res['res'] = (res.res2)




x = np.linspace(17, 40,100)*1e-3
def res_rec(x):
    dpix_x = 1.85e-6
    dpix_y = 1.85e-6
    Nx = 4024
    Ny = 3036
    
    Deff_pix = np.sqrt(dpix_x*dpix_y)
    Neff = np.sqrt(Nx*Ny)
    Deff = Deff_pix * Neff
    tmp = 2.44 * 410e-9 * x / Deff
    return tmp




plt.rcParams.update({'font.size': 15.5,
                     'font.weight': 'normal',
                     'axes.titlepad': 5.0,
                     'xtick.top': True,
                     'xtick.major.top':True,
                     'xtick.major.width': 1,
                     'xtick.major.size': 3,
                     'ytick.major.width': 1,
                     'ytick.major.size': 3,
                     'ytick.minor.width': 1,
                     'ytick.minor.size': 3,
                     'ytick.right': True})

plt.figure(figsize = (8.3,4.5))
plt.scatter(res.z,res.res, marker = 'x', color = 'firebrick', s = 40, label = '$D_{res,exp}$' )
plt.plot(x*1e3,res_rec(x)*1e6,lw = 1.8, color = 'black', label = '$D_{res,rec}$')
plt.hlines(1.85*2,6,40, lw = 1.8,linestyle = '--', color = 'black', label = '$D_{res,pix}$')
plt.ylim(2.5,7)
plt.xlabel('Reconstruction depth $z_{rec}$ (mm)')
plt.ylabel('Resolution ($\mathrm{\mu}$m)')
plt.legend(fontsize = 14)
plt.tight_layout()
plt.savefig('fig02.png', dpi = 300)



