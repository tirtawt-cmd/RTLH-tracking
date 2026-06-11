/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import { create } from 'zustand';
import { PaketPengadaan, SektorSpesifikasi, PaketStatus, KabupatenKalbar, DbPathType, UserProfile, UserRole, RekananInfo, ActivityLog, BackupEntry, AddendumKontrak, BpjsVerification } from './types';
import { DEFAULT_PAKET_DATA, PRESET_USERS, REKANAN_PRESET, KABUPATEN_LIST } from './constants';
import { collection, doc, setDoc, deleteDoc, onSnapshot, getDocs, writeBatch, getDoc, query, orderBy, limit, where } from 'firebase/firestore';
import { signInWithPopup, GoogleAuthProvider, signOut, onAuthStateChanged } from 'firebase/auth';
import { db, auth, handleFirestoreError, OperationType } from './firebase';
import firebaseConfig from '../firebase-applet-config.json';

interface PaketState {
  // Authentication & System State
  user: UserProfile | null;
  pendingGoogleUser: { uid: string; email: string; displayName: string } | null;
  dbMode: DbPathType;
  tahunAnggaran: number;
  connectionStatus: 'connected' | 'offline' | 'error';
  firebaseConfigured: boolean;
  
  // Data State
  pakets: PaketPengadaan[];
  rekanans: RekananInfo[];
  registeredUsers: UserProfile[];
  activityLogs: ActivityLog[];
  backups: BackupEntry[];
  bpjsVerifikasis: BpjsVerification[];
  
  // UI State / Filters
  searchQuery: string;
  statusFilter: string;
  sektorFilter: string;
  kabupatenFilter: string;
  
  // Actions
  login: (username: string, password?: string) => { success: boolean; message: string };
  loginWithGoogle: () => Promise<{ success: boolean; message: string; needsSetup?: boolean }>;
  completeGoogleRegistration: (role: UserRole, instansi: string) => Promise<{ success: boolean; message: string }>;
  logout: () => void | Promise<void>;
  setUser: (user: UserProfile | null) => void;
  setDbMode: (mode: DbPathType) => Promise<{ success: boolean; message: string }>;
  setTahunAnggaran: (year: number) => void;
  
  // CRUD Actions
  addPaket: (paket: Omit<PaketPengadaan, 'id' | 'createdAt' | 'updatedAt'>) => Promise<void>;
  updatePaket: (id: string, updated: Partial<PaketPengadaan>) => Promise<void>;
  deletePaket: (id: string) => Promise<void>;
  deletePakets: (ids: string[]) => Promise<void>;
  addActivityLog: (action: string, targetId: string, description: string) => Promise<void>;
  generateDemoData: (count?: number) => Promise<void>;
  clearAllData: () => Promise<void>;
  importPakets: (imported: Omit<PaketPengadaan, 'id' | 'createdAt' | 'updatedAt'>[]) => Promise<{ success: boolean; count: number; message: string }>;
  
  // Backup & Restore Actions
  fetchBackups: () => Promise<void>;
  backupData: (description: string, type?: 'Manual' | 'Auto-Import') => Promise<BackupEntry>;
  deleteBackup: (id: string) => Promise<void>;
  restoreData: (restoredPakets: PaketPengadaan[]) => Promise<{ success: boolean; message: string }>;
  
  // Addendum Actions
  addAddendum: (paketId: string, nomorAddendum: string, tanggalAddendum: string, nilaiKontrakBaru: number, tanggalSelesaiBaru: string, alasan: string) => Promise<void>;
  deleteAddendum: (paketId: string, addendumId: string) => Promise<void>;

  // Filters Actions
  setSearchQuery: (query: string) => void;
  setStatusFilter: (status: string) => void;
  setSektorFilter: (sektor: string) => void;
  setKabupatenFilter: (kab: string) => void;
  
  // User Registration (Registrasi Akun)
  fetchRegisteredUsers: () => Promise<void>;
  deleteRegisteredUser: (userId: string) => Promise<void>;
  registerUser: (email: string, fullName: string, role: UserRole, instansi: string, kabupaten1?: KabupatenKalbar, kabupaten2?: KabupatenKalbar) => Promise<{ success: boolean; message: string }>;
  verifyBpjsStatus: (paketId: string, statusVerifikasi: 'Belum Lunas' | 'Lunas', nomorBuktiIuran: string) => Promise<void>;
  searchBpjsVerifikasis: (queryType: 'kontrak' | 'npwp' | 'penyedia', queryValue: string) => Promise<BpjsVerification[]>;
  initBpjsVerification: (paketId: string) => Promise<void>;
}

let unsubscribePakets: (() => void) | null = null;
let unsubscribeLogs: (() => void) | null = null;
let unsubscribeBpjs: (() => void) | null = null;

const checkLocalFirebaseSetup = (): boolean => {
  return !!firebaseConfig.projectId;
};

// Sync BPJS verifikasi
const startBpjsSync = (set: any) => {
  if (unsubscribeBpjs) {
    unsubscribeBpjs();
    unsubscribeBpjs = null;
  }
  try {
    unsubscribeBpjs = onSnapshot(collection(db, 'bpjs_verifikasi'), (snapshot) => {
      const list: BpjsVerification[] = [];
      snapshot.forEach((docSnap) => {
        list.push(docSnap.data() as BpjsVerification);
      });
      set({ bpjsVerifikasis: list });
    }, (error) => {
      console.error('Firestore BPJS verifikasi sync failed:', error);
    });
  } catch (error) {
    console.error('Firestore BPJS sync error:', error);
  }
};

// FIX: startFirebaseSync sekarang hanya mensinkronkan pakets saja (tidak memerlukan auth)
const startPaketsSync = (set: any) => {
  if (unsubscribePakets) {
    unsubscribePakets();
    unsubscribePakets = null;
  }
  const path = 'pakets';
  try {
    unsubscribePakets = onSnapshot(collection(db, path), async (snapshot) => {
      if (snapshot.empty) {
        console.log('Firestore pakets collection is empty. Seeding default data...');
        try {
          const batch = writeBatch(db);
          const nowString = new Date().toISOString();
          DEFAULT_PAKET_DATA.forEach((p) => {
            const normalized = {
              ...p,
              sektor: normalizeSektorForDb(p.sektor) as SektorSpesifikasi,
              createdAt: p.createdAt || nowString,
              updatedAt: p.updatedAt || nowString
            };
            batch.set(doc(db, path, p.id), cleanUndefinedFields(normalized));
            
            const isContracted = ['Kontrak', 'Pelaksanaan', 'Selesai'].includes(p.status);
            if (isContracted) {
              const bpjsData = {
                id: p.id,
                namaKegiatan: p.namaKegiatan,
                nomorKontrak: p.nomorKontrak || '',
                nilaiKontrak: p.nilaiKontrak || 0,
                mitraPenyedia: p.mitraPenyedia || '',
                npwpPenyedia: p.npwpPenyedia || '',
                tanggalKontrak: p.tanggalKontrak || '',
                statusVerifikasi: 'Belum Lunas',
                nomorBuktiIuran: '',
                verifiedAt: '',
                verifiedBy: '',
                verifiedByName: '',
                updatedAt: nowString
              };
              batch.set(doc(db, 'bpjs_verifikasi', p.id), bpjsData);
            }
          });
          await batch.commit();
          console.log('Successfully seeded Firestore with default packages and BPJS data.');
        } catch (seedErr) {
          console.error('Failed to seed default packages to Firestore:', seedErr);
        }
        return;
      }

      const list: PaketPengadaan[] = [];
      snapshot.forEach((docSnap) => {
        const data = docSnap.data() as PaketPengadaan;
        if ((data.sektor as string) === 'Konsultansi') {
          const s = (data.namaKegiatan || '').toLowerCase();
          if (s.includes('supervisi') || s.includes('pengawasan')) {
            data.sektor = SektorSpesifikasi.KONSULTANSI_SUPERVISI;
          } else {
            data.sektor = SektorSpesifikasi.KONSULTANSI_PERENCANAAN;
          }
        }
        list.push(data);
      });
      list.sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime());
      set({ pakets: list, connectionStatus: 'connected' });
      // Cache list to local storage
      try {
        localStorage.setItem('silpja_pakets', JSON.stringify(list));
      } catch (e) {
        console.error('Failed to cache synced packets to local storage', e);
      }
    }, (error) => {
      set({ connectionStatus: 'error' });
      console.error('Firestore real-time sync failed:', error);
    });
  } catch (error) {
    set({ connectionStatus: 'error' });
    console.error('Firestore sync error:', error);
  }
};

// FIX: Listener log aktivitas dipisah — hanya dipanggil SETELAH user login
const startLogsSync = (set: any) => {
  if (unsubscribeLogs) {
    unsubscribeLogs();
    unsubscribeLogs = null;
  }

  try {
    const q = query(
      collection(db, 'activity_logs'),
      orderBy('timestamp', 'desc'),
      limit(150)
    );
    unsubscribeLogs = onSnapshot(q, (snapshot) => {
      const list: ActivityLog[] = [];
      snapshot.forEach((docSnap) => {
        list.push(docSnap.data() as ActivityLog);
      });
      set({ activityLogs: list });
      try {
        localStorage.setItem('silpja_activity_logs', JSON.stringify(list));
      } catch (e) {
        console.error('Failed to save activity logs to local storage', e);
      }
    }, (error) => {
      console.error('Firestore activity logs sync failed, mencoba fallback tanpa orderBy:', error);
      
      // Fallback: query tanpa orderBy jika index belum dibuat
      try {
        if (unsubscribeLogs) {
          unsubscribeLogs();
          unsubscribeLogs = null;
        }
        unsubscribeLogs = onSnapshot(collection(db, 'activity_logs'), (fallbackSnap) => {
          const list: ActivityLog[] = [];
          fallbackSnap.forEach((docSnap) => {
            list.push(docSnap.data() as ActivityLog);
          });
          list.sort((a, b) => {
            const timeA = a.timestamp ? new Date(a.timestamp).getTime() : 0;
            const timeB = b.timestamp ? new Date(b.timestamp).getTime() : 0;
            return timeB - timeA;
          });
          const limitedList = list.slice(0, 150);
          set({ activityLogs: limitedList });
          try {
            localStorage.setItem('silpja_activity_logs', JSON.stringify(limitedList));
          } catch (e) {
            console.error('Failed to save activity logs to local storage', e);
          }
        }, (fallbackError) => {
          console.error('Firestore activity logs fallback sync failed:', fallbackError);
        });
      } catch (fallbackError) {
        console.error('Firestore activity logs fallback query failed:', fallbackError);
      }
    });
  } catch (error) {
    console.error('Firestore activity logs sync error:', error);
  }
};

const stopFirebaseSync = () => {
  if (unsubscribePakets) {
    unsubscribePakets();
    unsubscribePakets = null;
  }
  if (unsubscribeLogs) {
    unsubscribeLogs();
    unsubscribeLogs = null;
  }
  if (unsubscribeBpjs) {
    unsubscribeBpjs();
    unsubscribeBpjs = null;
  }
};

const restartAllSyncs = (set: any, user: UserProfile | null) => {
  if (user) {
    if (user.role === 'BPJS') {
      if (unsubscribeLogs) {
        unsubscribeLogs();
        unsubscribeLogs = null;
      }
      startPaketsSync(set);
      startBpjsSync(set);
    } else {
      startPaketsSync(set);
      startLogsSync(set);
      startBpjsSync(set);
    }
  } else {
    stopFirebaseSync();
    if (checkLocalFirebaseSetup()) {
      startPaketsSync(set);
      startBpjsSync(set);
    }
  }
};

function cleanUndefinedFields<T extends object>(obj: T): T {
  const cleaned = { ...obj } as any;
  Object.keys(cleaned).forEach((key) => {
    if (cleaned[key] === undefined) {
      delete cleaned[key];
    }
  });
  return cleaned;
}

const normalizeSektorForDb = (sektor: string): string => {
  if (sektor === SektorSpesifikasi.KONSULTANSI_SUPERVISI || sektor === SektorSpesifikasi.KONSULTANSI_PERENCANAAN) {
    return 'Konsultansi';
  }
  return sektor;
};

const syncBpjsDoc = async (paket: PaketPengadaan, existingVer?: BpjsVerification) => {
  const isContracted = ['Kontrak', 'Pelaksanaan', 'Selesai'].includes(paket.status);
  const docRef = doc(db, 'bpjs_verifikasi', paket.id);
  if (isContracted) {
    const bpjsData = {
      id: paket.id,
      namaKegiatan: paket.namaKegiatan,
      nomorKontrak: paket.nomorKontrak || '',
      nilaiKontrak: paket.nilaiKontrak || 0,
      mitraPenyedia: paket.mitraPenyedia || '',
      npwpPenyedia: paket.npwpPenyedia || '',
      tanggalKontrak: paket.tanggalKontrak || '',
      statusVerifikasi: existingVer?.statusVerifikasi || 'Belum Lunas',
      nomorBuktiIuran: existingVer?.nomorBuktiIuran || '',
      verifiedAt: existingVer?.verifiedAt || '',
      verifiedBy: existingVer?.verifiedBy || '',
      verifiedByName: existingVer?.verifiedByName || '',
      updatedAt: new Date().toISOString()
    };
    await setDoc(docRef, bpjsData);
  } else {
    try {
      await deleteDoc(docRef);
    } catch (e) {}
  }
};

export const usePaketStore = create<PaketState>((set, get) => {
  const initializePackets = (): PaketPengadaan[] => {
    try {
      const stored = localStorage.getItem('silpja_pakets');
      if (stored) {
        return JSON.parse(stored);
      }
    } catch (e) {
      console.error('Failed to load local packets', e);
    }
    localStorage.setItem('silpja_pakets', JSON.stringify(DEFAULT_PAKET_DATA));
    return DEFAULT_PAKET_DATA;
  };

  const initializeActivityLogs = (): ActivityLog[] => {
    try {
      const stored = localStorage.getItem('silpja_activity_logs');
      if (stored) {
        return JSON.parse(stored);
      }
    } catch (e) {
      console.error('Failed to load local activity logs', e);
    }
    return [];
  };

  const initializeBackups = (): BackupEntry[] => {
    try {
      const stored = localStorage.getItem('silpbj_backups');
      if (stored) {
        return JSON.parse(stored);
      }
    } catch (e) {
      console.error('Failed to load local backups', e);
    }
    return [];
  };

  const initializeUser = (): UserProfile | null => {
    try {
      const stored = localStorage.getItem('silpja_active_user');
      if (stored) return JSON.parse(stored);
    } catch {}
    return null;
  };

  const activeMode = 'Online' as DbPathType;
  const activeYear = Number(localStorage.getItem('silpja_tahun_anggaran')) || 2025;

  const syncOffline = (pakets: PaketPengadaan[]) => {
    try {
      localStorage.setItem('silpja_pakets', JSON.stringify(pakets));
    } catch (e) {
      console.error(e);
    }
  };

  // FIX: Hanya mulai sinkron pakets saat startup — log sync dimulai setelah login
  if (activeMode === 'Online' && checkLocalFirebaseSetup()) {
    startPaketsSync(set);
    startBpjsSync(set);
    // Jika sudah ada user di localStorage (sesi sebelumnya), langsung mulai log sync juga
    const cachedUser = initializeUser();
    if (cachedUser) {
      startLogsSync(set);
    }
  }

  return {
    // Initial States
    user: initializeUser(),
    pendingGoogleUser: null,
    dbMode: activeMode,
    tahunAnggaran: activeYear,
    connectionStatus: activeMode === 'Online' ? 'connected' : 'offline',
    firebaseConfigured: checkLocalFirebaseSetup(),
    
    pakets: initializePackets(),
    rekanans: REKANAN_PRESET,
    registeredUsers: [],
    activityLogs: initializeActivityLogs(),
    backups: initializeBackups(),
    bpjsVerifikasis: [],
    
    searchQuery: '',
    statusFilter: 'all',
    sektorFilter: 'all',
    kabupatenFilter: 'all',

    // FIX: Helper internal untuk set user + start log sync setelah login
    login: (username) => {
      const trimmed = username.trim().toLowerCase();
      const found = PRESET_USERS.find(u => u.username.toLowerCase() === trimmed);
      
      if (found) {
        localStorage.setItem('silpja_active_user', JSON.stringify(found));
        set({ user: found });
        // Restart sync listeners according to user role
        restartAllSyncs(set, found);
        get().addActivityLog('LOGIN', found.uid, `Masuk ke sistem (Preset)`);
        return { success: true, message: `Berhasil masuk sebagai ${found.fullName} (${found.role})` };
      }
      
      if (username.length >= 3) {
        // Support direct BPJS manual login for easy local testing
        const isBpjsUser = username.toLowerCase().includes('bpjs');
        const manualUser: UserProfile = {
          uid: 'manual_' + Date.now(),
          username: username,
          fullName: isBpjsUser ? 'Petugas BPJS Ketenagakerjaan' : username.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' '),
          role: isBpjsUser ? 'BPJS' : 'Admin',
          instansi: isBpjsUser ? 'Kantor Cabang BPJS Kalbar' : 'Instansi Daerah Kalbar'
        };
        localStorage.setItem('silpja_active_user', JSON.stringify(manualUser));
        set({ user: manualUser });
        restartAllSyncs(set, manualUser);
        get().addActivityLog('LOGIN', manualUser.uid, `Masuk ke sistem (${manualUser.role} Baru Manual)`);
        return { success: true, message: `Berhasil sebagai ${manualUser.role}: ${manualUser.fullName}` };
      }

      return { success: false, message: 'Username tidak valid! Gunakan Preset Cepat.' };
    },

    loginWithGoogle: async () => {
      try {
        const provider = new GoogleAuthProvider();
        const result = await signInWithPopup(auth, provider);
        const firebaseUser = result.user;
        
        if (!firebaseUser) {
          return { success: false, message: 'Gagal mendapatkan data pengguna Google.' };
        }
        
        const emailLower = firebaseUser.email?.toLowerCase() || '';
        
        const userDocRef = doc(db, 'users', firebaseUser.uid);
        const snapshot = await getDoc(userDocRef);
        
        if (snapshot.exists()) {
          const profile = snapshot.data() as UserProfile;
          localStorage.setItem('silpja_active_user', JSON.stringify(profile));
          set({ user: profile, pendingGoogleUser: null });
          restartAllSyncs(set, profile);
          get().addActivityLog('LOGIN', profile.uid, `Masuk ke sistem (Google Auth)`);
          return { success: true, message: `Berhasil masuk sebagai ${profile.fullName} (${profile.role})` };
        }
        
        if (emailLower) {
          const emailDocRef = doc(db, 'users', emailLower);
          const emailSnap = await getDoc(emailDocRef);
          
          if (emailSnap.exists()) {
            const preProfile = emailSnap.data() as UserProfile;
            const completedProfile: UserProfile = {
              ...preProfile,
              uid: firebaseUser.uid,
              email: emailLower
            };
            
            await setDoc(userDocRef, cleanUndefinedFields(completedProfile));
            try {
              await deleteDoc(emailDocRef);
            } catch (err) {
              console.warn("Could not delete email placeholder:", err);
            }
            
            localStorage.setItem('silpja_active_user', JSON.stringify(completedProfile));
            set({ user: completedProfile, pendingGoogleUser: null });
            restartAllSyncs(set, completedProfile);
            get().addActivityLog('LOGIN', completedProfile.uid, `Menyelesaikan pendaftaran & masuk ke sistem`);
            return { success: true, message: `Berhasil mendaftarkan & masuk sebagai ${completedProfile.fullName} (${completedProfile.role})` };
          }
        }
        
        if (emailLower === 'tirtawt@gmail.com') {
          const profile: UserProfile = {
            uid: firebaseUser.uid,
            username: 'admin_tirta',
            fullName: firebaseUser.displayName || 'Tirta Wijaya',
            role: 'Admin',
            instansi: 'Sekretariat Dinas, DPRKP Kalbar',
            email: 'tirtawt@gmail.com'
          };
          await setDoc(userDocRef, profile);
          localStorage.setItem('silpja_active_user', JSON.stringify(profile));
          set({ user: profile, pendingGoogleUser: null });
          restartAllSyncs(set, profile);
          get().addActivityLog('LOGIN', profile.uid, `Masuk ke sistem sebagai Administrator Utama`);
          return { success: true, message: `Mendaftarkan & Masuk sebagai Administrator Utama: ${profile.fullName}` };
        }
        
        await signOut(auth);
        return { 
          success: false, 
          message: `Akun Google Anda (${emailLower || 'tanpa email'}) belum terdaftar di SiLPJA-Kalbar. Silakan hubungi Administrator untuk mendaftarkan akses.` 
        };
      } catch (error: any) {
        console.error('Google Auth Login Error:', error);
        return { success: false, message: `Gagal masuk dengan Google: ${error.message}` };
      }
    },

    completeGoogleRegistration: async (role, instansi) => {
      return { success: true, message: 'Fungsi registrasi mandiri telah dinonaktifkan.' };
    },

    logout: async () => {
      const currentUser = get().user;
      if (currentUser) {
        await get().addActivityLog('LOGOUT', currentUser.uid, `Keluar dari sistem`);
      }
      restartAllSyncs(set, null);
      try {
        await signOut(auth);
      } catch (e) {
        console.error('Sign-out error:', e);
      }
      localStorage.removeItem('silpja_active_user');
      set({ user: null, pendingGoogleUser: null, activityLogs: [] });
    },

    setUser: (user) => {
      if (user) {
        localStorage.setItem('silpja_active_user', JSON.stringify(user));
      } else {
        localStorage.removeItem('silpja_active_user');
      }
      restartAllSyncs(set, user);
      set({ user });
    },

    setDbMode: async (mode) => {
      return {
        success: true,
        message: 'Aplikasi beroperasi secara penuh di Cloud Firebase Server (Online Sync).'
      };
    },

    setTahunAnggaran: (year) => {
      localStorage.setItem('silpja_tahun_anggaran', String(year));
      set({ tahunAnggaran: year });
    },

    // CRUD Operations
    addPaket: async (newFields) => {
      const newId = `PKT-${get().tahunAnggaran}-${Math.floor(100 + Math.random() * 900)}`;
      const nowString = new Date().toISOString();
      const newPaket: PaketPengadaan = {
        ...newFields,
        sektor: normalizeSektorForDb(newFields.sektor) as SektorSpesifikasi,
        id: newId,
        createdAt: nowString,
        updatedAt: nowString
      };

      const path = `pakets`;
      try {
        await setDoc(doc(db, path, newId), cleanUndefinedFields(newPaket));
        await syncBpjsDoc(newPaket);
        await get().addActivityLog('CREATE', newId, `Menambahkan paket baru: ${newPaket.namaKegiatan} (Pagu: Rp ${newPaket.paguHPS.toLocaleString('id-ID')})`);
      } catch (error) {
        handleFirestoreError(error, OperationType.CREATE, `${path}/${newId}`);
      }
    },

    updatePaket: async (id, updatedFields) => {
      const nowString = new Date().toISOString();
      const path = `pakets`;
      try {
        const existingPaket = get().pakets.find(p => p.id === id);
        if (existingPaket) {
          let diffStr = '';
          if (updatedFields.progresFisik !== undefined && updatedFields.progresFisik !== existingPaket.progresFisik) {
            diffStr += `progres fisik (${existingPaket.progresFisik}% -> ${updatedFields.progresFisik}%)`;
          }
          if (updatedFields.catatanKhusus !== undefined && updatedFields.catatanKhusus !== existingPaket.catatanKhusus) {
            diffStr += (diffStr ? ', ' : '') + `catatan khusus ('${existingPaket.catatanKhusus || '-'}' -> '${updatedFields.catatanKhusus || '-'}')`;
          }
          const otherFields = Object.keys(updatedFields).filter(k => k !== 'progresFisik' && k !== 'catatanKhusus' && k !== 'updatedAt');
          if (otherFields.length > 0) {
            diffStr += (diffStr ? ', ' : '') + `informasi teknis paket (${otherFields.join(', ')})`;
          }

          const updatedSektor = updatedFields.sektor ? normalizeSektorForDb(updatedFields.sektor) : undefined;
          const updatedPaketObj = {
            ...existingPaket,
            ...updatedFields,
            ...(updatedSektor ? { sektor: updatedSektor } : {}),
            updatedAt: nowString
          };
          await setDoc(doc(db, path, id), cleanUndefinedFields(updatedPaketObj));
          
          const existingVer = get().bpjsVerifikasis.find(v => v.id === id);
          await syncBpjsDoc(updatedPaketObj, existingVer);
          await get().addActivityLog('UPDATE', id, `Mengubah paket ${id}: ${diffStr || 'menyimpan data'}`);
        }
      } catch (error) {
        handleFirestoreError(error, OperationType.UPDATE, `${path}/${id}`);
      }
    },

    deletePaket: async (id) => {
      const path = `pakets`;
      try {
        await deleteDoc(doc(db, path, id));
        try {
          await deleteDoc(doc(db, 'bpjs_verifikasi', id));
        } catch (e) {}
        await get().addActivityLog('DELETE', id, `Menghapus paket: ${id}`);
      } catch (error) {
        handleFirestoreError(error, OperationType.DELETE, `${path}/${id}`);
      }
    },

    deletePakets: async (ids) => {
      const path = `pakets`;
      try {
        const chunkSize = 400;
        for (let i = 0; i < ids.length; i += chunkSize) {
          const chunk = ids.slice(i, i + chunkSize);
          const batch = writeBatch(db);
          chunk.forEach((id) => {
            batch.delete(doc(db, path, id));
            batch.delete(doc(db, 'bpjs_verifikasi', id));
          });
          await batch.commit();
        }
        await get().addActivityLog('DELETE_BATCH', ids.join(', '), `Menghapus massal ${ids.length} paket: ${ids.join(', ')}`);
      } catch (error) {
        handleFirestoreError(error, OperationType.DELETE, path);
      }
    },

    addActivityLog: async (action, targetId, description) => {
      const user = get().user;
      // FIX: Guard — jangan tulis log jika tidak ada user
      if (!user) {
        console.warn('[ActivityLog] Diabaikan — tidak ada user yang login:', action, description);
        return;
      }

      const logId = `LOG-${Date.now()}-${Math.floor(100 + Math.random() * 900)}`;
      const logEntry: ActivityLog = {
        id: logId,
        userId: user.uid || user.email || 'unknown',
        userFullName: user.fullName || 'Unknown Operator',
        userRole: user.role,
        action,
        targetId,
        description,
        timestamp: new Date().toISOString()
      };

      // FIX: Update state lokal DULU (optimistic update) agar langsung terlihat di UI
      const currentLogs = get().activityLogs;
      const updatedLogs = [logEntry, ...currentLogs].slice(0, 150);
      set({ activityLogs: updatedLogs });
      try {
        localStorage.setItem('silpja_activity_logs', JSON.stringify(updatedLogs));
      } catch (e) {}

      // Lalu simpan ke Firestore (async, tidak blocking UI)
      try {
        await setDoc(doc(db, 'activity_logs', logId), logEntry);
      } catch (err) {
        console.error('Gagal menyimpan log aktivitas ke Firestore:', err);
        // Log tetap ada di state lokal meskipun Firestore gagal
      }
    },

    generateDemoData: async () => {
      // Disabled
    },

    clearAllData: async () => {
      const path = `pakets`;
      try {
        const snapshot = await getDocs(collection(db, path));
        const batch = writeBatch(db);
        snapshot.forEach((dt) => {
          batch.delete(doc(db, path, dt.id));
        });
        await batch.commit();
        set({ pakets: [] });
      } catch (error) {
        handleFirestoreError(error, OperationType.DELETE, path);
      }
    },

    importPakets: async (importedList) => {
      const currentYear = get().tahunAnggaran;
      const nowString = new Date().toISOString();
      
      const newPakets: PaketPengadaan[] = importedList.map((item, index) => {
        const randId = `PKT-${currentYear}-${Math.floor(100 + Math.random() * 899)}-IM${index}`;
        return {
          id: randId.substring(0, 50),
          namaKegiatan: (item.namaKegiatan || 'Kepanitiaan Konstruksi Baru').substring(0, 500),
          kodeSIRUP: (item.kodeSIRUP || Math.floor(1000000000 + Math.random() * 8999999999).toString()).substring(0, 50),
          sektor: normalizeSektorForDb(item.sektor || SektorSpesifikasi.KONSTRUKSI) as SektorSpesifikasi,
          paguHPS: Number(item.paguHPS) || 0,
          nilaiKontrak: Number(item.nilaiKontrak) || 0,
          mitraPenyedia: (item.mitraPenyedia || '-').substring(0, 200),
          progresFisik: Number(item.progresFisik) || 0,
          status: item.status || PaketStatus.PERSIAPAN,
          kabupaten: (item.kabupaten || 'Kota Pontianak').substring(0, 100) as KabupatenKalbar,
          tahunAnggaran: Number(item.tahunAnggaran) || currentYear,
          createdAt: nowString,
          updatedAt: nowString,
          latitude: item.latitude !== undefined && item.latitude !== null && !isNaN(Number(item.latitude)) ? Number(item.latitude) : undefined,
          longitude: item.longitude !== undefined && item.longitude !== null && !isNaN(Number(item.longitude)) ? Number(item.longitude) : undefined,
          nomorPaket: item.nomorPaket || '',
          sumberDana: item.sumberDana || '',
          programKegiatan: item.programKegiatan || '',
          subKegiatan: item.subKegiatan || '',
          jenisPengadaan: item.jenisPengadaan || '',
          metodePemilihan: item.metodePemilihan || '',
          nilaiPenawaran: item.nilaiPenawaran !== undefined && item.nilaiPenawaran !== null && !isNaN(Number(item.nilaiPenawaran)) ? Number(item.nilaiPenawaran) : 0,
          nomorKontrak: item.nomorKontrak || '',
          tanggalKontrak: item.tanggalKontrak || '',
          tanggalMulai: item.tanggalMulai || '',
          tanggalSelesai: item.tanggalSelesai || '',
          durasiHari: item.durasiHari !== undefined && item.durasiHari !== null && !isNaN(Number(item.durasiHari)) ? Number(item.durasiHari) : 0,
          npwpPenyedia: item.npwpPenyedia || '',
          direkturPenyedia: item.direkturPenyedia || '',
          kontakPenyedia: item.kontakPenyedia || '',
          alamatPenyedia: item.alamatPenyedia || '',
          lokasiPekerjaan: item.lokasiPekerjaan || '',
          catatanKhusus: item.catatanKhusus || '',
          namaAdmin: item.namaAdmin || '',
          namaDewan: item.namaDewan || '',
          verifikasiPPTKUangMuka: false,
          tanggalKwitansiUangMuka: '',
          nomorKwitansiUangMuka: '',
          tanggalSPPUangMuka: '',
          nomorSPPUangMuka: '',
          catatanUangMuka: '',
          verifikasiPPTK100: false,
          tanggalKwitansi100: '',
          nomorKwitansi100: '',
          tanggalSPP100: '',
          nomorSPP100: '',
          catatan100: '',
          catatanPPTK: '',
          tanggalVerifikasiPPTK: '',
          laporanMingguan: []
        };
      });
      const path = `pakets`;
      try {
        const chunkSize = 400;
        for (let i = 0; i < newPakets.length; i += chunkSize) {
          const chunk = newPakets.slice(i, i + chunkSize);
          const batch = writeBatch(db);
          chunk.forEach((p) => {
            batch.set(doc(db, path, p.id), cleanUndefinedFields(p));
            const isContracted = ['Kontrak', 'Pelaksanaan', 'Selesai'].includes(p.status);
            if (isContracted) {
              const bpjsData = {
                id: p.id,
                namaKegiatan: p.namaKegiatan,
                nomorKontrak: p.nomorKontrak || '',
                nilaiKontrak: p.nilaiKontrak || 0,
                mitraPenyedia: p.mitraPenyedia || '',
                npwpPenyedia: p.npwpPenyedia || '',
                tanggalKontrak: p.tanggalKontrak || '',
                statusVerifikasi: 'Belum Lunas',
                nomorBuktiIuran: '',
                verifiedAt: '',
                verifiedBy: '',
                verifiedByName: '',
                updatedAt: nowString
              };
              batch.set(doc(db, 'bpjs_verifikasi', p.id), bpjsData);
            }
          });
          await batch.commit();
        }

        try {
          await get().backupData(`Otomatis setelah import ${newPakets.length} paket`, 'Auto-Import');
        } catch (backupErr) {
          console.error('Auto-backup after import failed:', backupErr);
        }

        return { success: true, count: newPakets.length, message: `Berhasil mengimpor ${newPakets.length} paket ke Cloud Firestore.` };
      } catch (error) {
        console.error(error);
        return { success: false, count: 0, message: `Gagal mengimpor ke Firestore: ${error instanceof Error ? error.message : String(error)}` };
      }
    },

    // Backup & Restore Actions
    fetchBackups: async () => {
      try {
        const querySnapshot = await getDocs(collection(db, 'backups'));
        const list: BackupEntry[] = [];
        querySnapshot.forEach((docSnap) => {
          list.push(docSnap.data() as BackupEntry);
        });
        list.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
        set({ backups: list });
        localStorage.setItem('silpbj_backups', JSON.stringify(list));
      } catch (err) {
        console.error('Failed to fetch backups from Firestore:', err);
        try {
          const stored = localStorage.getItem('silpbj_backups');
          if (stored) {
            set({ backups: JSON.parse(stored) });
          }
        } catch (e) {
          console.error('Failed to load local backups fallback', e);
        }
      }
    },

    backupData: async (description, type = 'Manual') => {
      const user = get().user;
      const pakets = get().pakets;
      const backupId = `BKP-${Date.now()}`;
      
      const backupEntry: BackupEntry = {
        id: backupId,
        timestamp: new Date().toISOString(),
        createdByName: user?.fullName || 'Sistem',
        createdByUid: user?.uid || 'system',
        description,
        totalPakets: pakets.length,
        data: {
          pakets: pakets.map(p => cleanUndefinedFields(p))
        },
        type
      };

      try {
        const stored = localStorage.getItem('silpbj_backups');
        const list: BackupEntry[] = stored ? JSON.parse(stored) : [];
        const updatedList = [backupEntry, ...list].slice(0, 50);
        localStorage.setItem('silpbj_backups', JSON.stringify(updatedList));
        set({ backups: updatedList });
      } catch (e) {
        console.error('Failed to save backup to local storage', e);
      }

      try {
        await setDoc(doc(db, 'backups', backupId), cleanUndefinedFields({
          ...backupEntry,
          data: {
            pakets: backupEntry.data.pakets.map(p => cleanUndefinedFields(p))
          }
        }));
        await get().addActivityLog('BACKUP_DATA', backupId, `Membuat restore point: ${description} (${pakets.length} paket)`);
      } catch (err) {
        console.error('Failed to save backup to Firestore:', err);
        throw err;
      }

      return backupEntry;
    },

    deleteBackup: async (backupId) => {
      try {
        await deleteDoc(doc(db, 'backups', backupId));
        await get().addActivityLog('DELETE_BACKUP', backupId, `Menghapus restore point: ${backupId}`);
      } catch (err) {
        console.error('Failed to delete backup from Firestore:', err);
      }

      try {
        const stored = localStorage.getItem('silpbj_backups');
        if (stored) {
          const list: BackupEntry[] = JSON.parse(stored);
          const updated = list.filter(b => b.id !== backupId);
          localStorage.setItem('silpbj_backups', JSON.stringify(updated));
          set({ backups: updated });
        }
      } catch (e) {
        console.error(e);
      }
    },

    restoreData: async (restoredPakets) => {
      const path = 'pakets';
      const currentYear = get().tahunAnggaran;
      try {
        const querySnapshot = await getDocs(collection(db, path));
        const deleteIds: string[] = [];
        querySnapshot.forEach((docSnap) => {
          deleteIds.push(docSnap.id);
        });
        
        if (deleteIds.length > 0) {
          const chunkSize = 400;
          for (let i = 0; i < deleteIds.length; i += chunkSize) {
            const chunk = deleteIds.slice(i, i + chunkSize);
            const batch = writeBatch(db);
            chunk.forEach((id) => {
              batch.delete(doc(db, path, id));
            });
            await batch.commit();
          }
        }

        // Clean all bpjs verifications
        const bpjsQuerySnapshot = await getDocs(collection(db, 'bpjs_verifikasi'));
        const deleteBpjsIds: string[] = [];
        bpjsQuerySnapshot.forEach((docSnap) => {
          deleteBpjsIds.push(docSnap.id);
        });
        if (deleteBpjsIds.length > 0) {
          const chunkSize = 400;
          for (let i = 0; i < deleteBpjsIds.length; i += chunkSize) {
            const chunk = deleteBpjsIds.slice(i, i + chunkSize);
            const batch = writeBatch(db);
            chunk.forEach((id) => {
              batch.delete(doc(db, 'bpjs_verifikasi', id));
            });
            await batch.commit();
          }
        }

        if (restoredPakets.length > 0) {
          const chunkSize = 400;
          for (let i = 0; i < restoredPakets.length; i += chunkSize) {
            const chunk = restoredPakets.slice(i, i + chunkSize);
            const batch = writeBatch(db);
            chunk.forEach((p) => {
              const normalized = {
                ...p,
                sektor: normalizeSektorForDb(p.sektor) as SektorSpesifikasi,
                tahunAnggaran: p.tahunAnggaran || currentYear
              };
              batch.set(doc(db, path, p.id), cleanUndefinedFields(normalized));

              // Restore sync bpjs doc
              const isContracted = ['Kontrak', 'Pelaksanaan', 'Selesai'].includes(p.status);
              if (isContracted) {
                const bpjsData = {
                  id: p.id,
                  namaKegiatan: p.namaKegiatan,
                  nomorKontrak: p.nomorKontrak || '',
                  nilaiKontrak: p.nilaiKontrak || 0,
                  mitraPenyedia: p.mitraPenyedia || '',
                  npwpPenyedia: p.npwpPenyedia || '',
                  tanggalKontrak: p.tanggalKontrak || '',
                  statusVerifikasi: 'Belum Lunas',
                  nomorBuktiIuran: '',
                  verifiedAt: '',
                  verifiedBy: '',
                  verifiedByName: '',
                  updatedAt: new Date().toISOString()
                };
                batch.set(doc(db, 'bpjs_verifikasi', p.id), bpjsData);
              }
            });
            await batch.commit();
          }
        }

        await get().addActivityLog('RESTORE_DATA', `RESTORE-${Date.now()}`, `Memulihkan database: ${restoredPakets.length} paket dipulihkan`);
        return { success: true, message: `Berhasil memulihkan ${restoredPakets.length} paket data.` };
      } catch (error: any) {
        console.error('Failed to restore database:', error);
        return { success: false, message: `Gagal memulihkan database: ${error.message}` };
      }
    },

    // Addendum Actions
    addAddendum: async (paketId, nomorAddendum, tanggalAddendum, nilaiKontrakBaru, tanggalSelesaiBaru, alasan) => {
      const path = 'pakets';
      const existingPaket = get().pakets.find(p => p.id === paketId);
      if (!existingPaket) return;

      const nowString = new Date().toISOString();
      const addendumId = `ADD-${Date.now()}-${Math.floor(100 + Math.random() * 900)}`;
      
      const newAddendum: AddendumKontrak = {
        id: addendumId,
        nomorAddendum,
        tanggalAddendum,
        nilaiKontrakBaru,
        tanggalSelesaiBaru,
        alasan,
        createdAt: nowString
      };

      const originalNilai = existingPaket.nilaiKontrakAwal || existingPaket.nilaiKontrak || 0;
      const originalSelesai = existingPaket.tanggalSelesaiAwal || existingPaket.tanggalSelesai || '';
      const originalDurasi = existingPaket.durasiHariAwal || existingPaket.durasiHari || 0;

      const currentAddendums = existingPaket.addendum || [];
      const updatedAddendums = [...currentAddendums, newAddendum];

      let calculatedDurasi = existingPaket.durasiHari;
      if (tanggalSelesaiBaru && existingPaket.tanggalMulai) {
        const [sy, sm, sd] = existingPaket.tanggalMulai.split('-').map(Number);
        const [ey, em, ed] = tanggalSelesaiBaru.split('-').map(Number);
        if (sy && sm && sd && ey && em && ed) {
          const start = Date.UTC(sy, sm - 1, sd);
          const end = Date.UTC(ey, em - 1, ed);
          const diffDays = Math.round((end - start) / (1000 * 60 * 60 * 24)) + 1;
          calculatedDurasi = diffDays >= 1 ? diffDays : 0;
        }
      }

      const updatedPaket: PaketPengadaan = {
        ...existingPaket,
        nilaiKontrakAwal: originalNilai,
        tanggalSelesaiAwal: originalSelesai,
        durasiHariAwal: originalDurasi,
        nilaiKontrak: nilaiKontrakBaru,
        tanggalSelesai: tanggalSelesaiBaru,
        durasiHari: calculatedDurasi,
        addendum: updatedAddendums,
        updatedAt: nowString
      };

      try {
        await setDoc(doc(db, path, paketId), cleanUndefinedFields(updatedPaket));
        await get().addActivityLog('ADD_ADDENDUM', paketId, `Menambahkan addendum ${nomorAddendum} untuk paket ${paketId}: Nilai Kontrak menjadi Rp ${nilaiKontrakBaru.toLocaleString('id-ID')}, tanggal selesai menjadi ${tanggalSelesaiBaru}`);
      } catch (error) {
        handleFirestoreError(error, OperationType.UPDATE, `${path}/${paketId}`);
      }
    },

    deleteAddendum: async (paketId, addendumId) => {
      const path = 'pakets';
      const existingPaket = get().pakets.find(p => p.id === paketId);
      if (!existingPaket) return;

      const nowString = new Date().toISOString();
      const currentAddendums = existingPaket.addendum || [];
      const updatedAddendums = currentAddendums.filter(a => a.id !== addendumId);

      let targetNilai = existingPaket.nilaiKontrakAwal || existingPaket.nilaiKontrak || 0;
      let targetSelesai = existingPaket.tanggalSelesaiAwal || existingPaket.tanggalSelesai || '';
      let targetDurasi = existingPaket.durasiHariAwal || existingPaket.durasiHari || 0;

      if (updatedAddendums.length > 0) {
        const latestAddendum = updatedAddendums[updatedAddendums.length - 1];
        targetNilai = latestAddendum.nilaiKontrakBaru;
        targetSelesai = latestAddendum.tanggalSelesaiBaru;
        
        if (targetSelesai && existingPaket.tanggalMulai) {
          const [sy, sm, sd] = existingPaket.tanggalMulai.split('-').map(Number);
          const [ey, em, ed] = targetSelesai.split('-').map(Number);
          if (sy && sm && sd && ey && em && ed) {
            const start = Date.UTC(sy, sm - 1, sd);
            const end = Date.UTC(ey, em - 1, ed);
            targetDurasi = Math.round((end - start) / (1000 * 60 * 60 * 24)) + 1;
            if (targetDurasi < 1) targetDurasi = 0;
          }
        }
      }

      const updatedPaket: PaketPengadaan = {
        ...existingPaket,
        nilaiKontrak: targetNilai,
        tanggalSelesai: targetSelesai,
        durasiHari: targetDurasi,
        addendum: updatedAddendums,
        updatedAt: nowString
      };

      try {
        await setDoc(doc(db, path, paketId), cleanUndefinedFields(updatedPaket));
        await get().addActivityLog('DELETE_ADDENDUM', paketId, `Menghapus addendum dari paket ${paketId}. Nilai kontrak disesuaikan menjadi Rp ${targetNilai.toLocaleString('id-ID')}, tanggal selesai ${targetSelesai}`);
      } catch (error) {
        handleFirestoreError(error, OperationType.UPDATE, `${path}/${paketId}`);
      }
    },
    // Filter Setters
    setSearchQuery: (query) => set({ searchQuery: query }),
    setStatusFilter: (status) => set({ statusFilter: status }),
    setSektorFilter: (sektor) => set({ sektorFilter: sektor }),
    setKabupatenFilter: (kab) => set({ kabupatenFilter: kab }),

    // User account registration
    fetchRegisteredUsers: async () => {
      try {
        const querySnapshot = await getDocs(collection(db, 'users'));
        if (querySnapshot.empty) {
          console.log('Firestore users collection is empty. Seeding default admin...');
          const path = 'users';
          const defaultAdmin: UserProfile = {
            uid: 'admin_tirta_placeholder',
            username: 'admin_tirta',
            fullName: 'Tirta Wijaya',
            role: 'Admin',
            instansi: 'Sekretariat Dinas, DPRKP Kalbar',
            email: 'tirtawt@gmail.com'
          };
          try {
            await setDoc(doc(db, path, 'tirtawt@gmail.com'), defaultAdmin);
            console.log('Successfully seeded default admin user.');
            set({ registeredUsers: [defaultAdmin] });
          } catch (seedErr) {
            console.error('Failed to seed default admin user to Firestore:', seedErr);
          }
          return;
        }

        const usersList: UserProfile[] = [];
        querySnapshot.forEach((documentSnap) => {
          usersList.push(documentSnap.data() as UserProfile);
        });
        set({ registeredUsers: usersList });
      } catch (err) {
        console.error('Failed to fetch registered users from Firestore:', err);
      }
    },

    deleteRegisteredUser: async (userId) => {
      try {
        await deleteDoc(doc(db, 'users', userId));
        set((state) => ({
          registeredUsers: state.registeredUsers.filter((u) => u.uid !== userId && u.email !== userId && u.username !== userId)
        }));
      } catch (err) {
        console.error('Failed to delete user:', err);
      }
    },

    registerUser: async (email, fullName, role, instansi, kabupaten1, kabupaten2) => {
      const cleanEmail = email.trim().toLowerCase();
      const cleanUsername = cleanEmail.split('@')[0].replace(/[^a-zA-Z0-9_]/g, '') || 'user_' + Date.now();
      
      const newProfile: UserProfile = {
        uid: cleanEmail,
        username: cleanUsername,
        fullName,
        role,
        instansi,
        email: cleanEmail,
        kabupaten1: kabupaten1 || undefined,
        kabupaten2: kabupaten2 || undefined
      };

      try {
        await setDoc(doc(db, 'users', cleanEmail), cleanUndefinedFields(newProfile));
        await get().fetchRegisteredUsers();
        await get().addActivityLog('REGISTER_USER', cleanEmail, `Mendaftarkan user baru: ${fullName} (${role})`);
        return { success: true, message: `Pra-registrasi berhasil! Pengguna dengan email ${cleanEmail} kini dapat masuk menggunakan Google.` };
      } catch (error: any) {
        console.error('Pre-registration error:', error);
        return { success: false, message: `Gagal mendaftarkan email: ${error.message}` };
      }
    },

    verifyBpjsStatus: async (paketId, statusVerifikasi, nomorBuktiIuran) => {
      const user = get().user;
      if (!user) {
        throw new Error('Anda harus login terlebih dahulu.');
      }
      if (user.role !== 'BPJS' && user.role !== 'Admin' && user.role !== 'PPK' && user.role !== 'PPTK') {
        throw new Error('Anda tidak memiliki wewenang untuk melakukan verifikasi ini.');
      }

      let existingVer = get().bpjsVerifikasis.find(v => v.id === paketId);
      if (!existingVer) {
        const paket = get().pakets.find(p => p.id === paketId);
        if (!paket) {
          throw new Error('Data verifikasi paket pekerjaan tidak ditemukan.');
        }
        existingVer = {
          id: paket.id,
          namaKegiatan: paket.namaKegiatan,
          nomorKontrak: paket.nomorKontrak || '',
          nilaiKontrak: paket.nilaiKontrak || 0,
          mitraPenyedia: paket.mitraPenyedia || '',
          npwpPenyedia: paket.npwpPenyedia || '',
          tanggalKontrak: paket.tanggalKontrak || '',
          statusVerifikasi: 'Belum Lunas',
          nomorBuktiIuran: '',
          verifiedAt: '',
          verifiedBy: '',
          verifiedByName: '',
          updatedAt: ''
        };
      }

      const now = new Date().toISOString();
      const updatedVer: BpjsVerification = {
        ...existingVer,
        statusVerifikasi,
        nomorBuktiIuran: statusVerifikasi === 'Lunas' ? nomorBuktiIuran : '',
        verifiedAt: now,
        verifiedBy: user.uid,
        verifiedByName: user.fullName,
        updatedAt: now
      };

      try {
        await setDoc(doc(db, 'bpjs_verifikasi', paketId), cleanUndefinedFields(updatedVer));
        
        // Update local state for BPJS role
        if (user.role === 'BPJS') {
          const currentList = get().bpjsVerifikasis;
          const exists = currentList.some(v => v.id === paketId);
          const updatedList = exists 
            ? currentList.map(v => v.id === paketId ? updatedVer : v)
            : [...currentList, updatedVer];
          set({ bpjsVerifikasis: updatedList });
        }

        await get().addActivityLog('VERIFY_BPJS', paketId, `Verifikasi BPJS: ${statusVerifikasi} (${statusVerifikasi === 'Lunas' ? 'Bukti: ' + nomorBuktiIuran : 'Dibatalkan'})`);
      } catch (error: any) {
        console.error('Failed to save BPJS verification:', error);
        throw new Error(`Gagal menyimpan verifikasi BPJS: ${error.message}`);
      }
    },

    searchBpjsVerifikasis: async (queryType, queryValue) => {
      const user = get().user;
      if (!user) throw new Error('Anda harus login terlebih dahulu.');
      
      const cleanValue = queryValue.trim();
      if (cleanValue.length < 3) {
        throw new Error('Kata kunci pencarian minimal harus 3 karakter.');
      }
      
      const collRef = collection(db, 'bpjs_verifikasi');
      let q;
      
      if (queryType === 'npwp') {
        q = query(collRef, where('npwpPenyedia', '==', cleanValue), limit(5));
      } else if (queryType === 'kontrak') {
        q = query(
          collRef, 
          where('nomorKontrak', '>=', cleanValue), 
          where('nomorKontrak', '<=', cleanValue + '\uf8ff'), 
          limit(5)
        );
      } else { // 'penyedia'
        q = query(
          collRef, 
          where('mitraPenyedia', '>=', cleanValue), 
          where('mitraPenyedia', '<=', cleanValue + '\uf8ff'), 
          limit(5)
        );
      }

      try {
        const querySnapshot = await getDocs(q);
        const results: BpjsVerification[] = [];
        querySnapshot.forEach((docSnap) => {
          results.push(docSnap.data() as BpjsVerification);
        });
        
        // Set results to local state so details can be loaded & verifyBpjsStatus can check it
        set({ bpjsVerifikasis: results });
        
        // Catat log aktivitas pencarian untuk audit keamanan
        await get().addActivityLog(
          'BPJS_SEARCH', 
          queryType + '_' + (cleanValue.length > 15 ? cleanValue.substring(0, 15) : cleanValue), 
          `Mencari data BPJS dengan tipe "${queryType}" dan kata kunci: "${cleanValue}"`
        );
        
        return results;
      } catch (err: any) {
        console.error('Pencarian data BPJS gagal:', err);
        throw new Error('Gagal memuat data dari server. Pastikan Anda terkoneksi ke server dan memiliki hak akses valid.');
      }
    },

    initBpjsVerification: async (paketId) => {
      const user = get().user;
      if (!user) {
        throw new Error('Anda harus login terlebih dahulu.');
      }
      if (user.role !== 'Admin' && user.role !== 'PPK' && user.role !== 'PPTK') {
        throw new Error('Anda tidak memiliki wewenang untuk melakukan inisialisasi ini.');
      }

      const paket = get().pakets.find(p => p.id === paketId);
      if (!paket) {
        throw new Error('Paket pekerjaan tidak ditemukan.');
      }

      const docRef = doc(db, 'bpjs_verifikasi', paket.id);
      const bpjsData: BpjsVerification = {
        id: paket.id,
        namaKegiatan: paket.namaKegiatan,
        nomorKontrak: paket.nomorKontrak || '',
        nilaiKontrak: paket.nilaiKontrak || 0,
        mitraPenyedia: paket.mitraPenyedia || '',
        npwpPenyedia: paket.npwpPenyedia || '',
        tanggalKontrak: paket.tanggalKontrak || '',
        statusVerifikasi: 'Belum Lunas',
        nomorBuktiIuran: '',
        verifiedAt: '',
        verifiedBy: '',
        verifiedByName: '',
        updatedAt: new Date().toISOString()
      };

      try {
        await setDoc(docRef, cleanUndefinedFields(bpjsData));
        await get().addActivityLog('INIT_BPJS', paketId, `Inisialisasi verifikasi BPJS untuk paket: ${paket.namaKegiatan}`);
      } catch (error: any) {
        console.error('Failed to initialize BPJS verification:', error);
        throw new Error(`Gagal menginisialisasi data BPJS: ${error.message}`);
      }
    }
  };
});

if (checkLocalFirebaseSetup()) {
  onAuthStateChanged(auth, async (firebaseUser) => {
    if (firebaseUser) {
      const cached = localStorage.getItem('silpja_active_user');
      if (cached) {
        const parsed = JSON.parse(cached);
        if (parsed.uid === firebaseUser.uid) {
          usePaketStore.setState({ user: parsed, pendingGoogleUser: null });
          restartAllSyncs((state: any) => usePaketStore.setState(state), parsed);
        }
      }
      
      try {
        const userDocRef = doc(db, 'users', firebaseUser.uid);
        const snapshot = await getDoc(userDocRef);
        
        if (snapshot.exists()) {
          const profile = snapshot.data() as UserProfile;
          localStorage.setItem('silpja_active_user', JSON.stringify(profile));
          usePaketStore.setState({ user: profile, pendingGoogleUser: null });
          restartAllSyncs((state: any) => usePaketStore.setState(state), profile);
        } else {
          const emailLower = firebaseUser.email?.toLowerCase() || '';
          
          if (emailLower) {
            const emailDocRef = doc(db, 'users', emailLower);
            const emailSnap = await getDoc(emailDocRef);
            
            if (emailSnap.exists()) {
              const preProfile = emailSnap.data() as UserProfile;
              const completedProfile: UserProfile = {
                ...preProfile,
                uid: firebaseUser.uid,
                email: emailLower
              };
              await setDoc(userDocRef, cleanUndefinedFields(completedProfile));
              try {
                await deleteDoc(emailDocRef);
              } catch (delErr) {
                console.warn(delErr);
              }
              
              localStorage.setItem('silpja_active_user', JSON.stringify(completedProfile));
              usePaketStore.setState({ user: completedProfile, pendingGoogleUser: null });
              restartAllSyncs((state: any) => usePaketStore.setState(state), completedProfile);
              return;
            }
          }

          if (emailLower === 'tirtawt@gmail.com') {
            const profile: UserProfile = {
              uid: firebaseUser.uid,
              username: 'admin_tirta',
              fullName: firebaseUser.displayName || 'Tirta Wijaya',
              role: 'Admin',
              instansi: 'Sekretariat Dinas, DPRKP Kalbar',
              email: 'tirtawt@gmail.com'
            };
            await setDoc(userDocRef, profile);
            localStorage.setItem('silpja_active_user', JSON.stringify(profile));
            usePaketStore.setState({ user: profile, pendingGoogleUser: null });
            restartAllSyncs((state: any) => usePaketStore.setState(state), profile);
          } else {
            await signOut(auth);
            localStorage.removeItem('silpja_active_user');
            usePaketStore.setState({ user: null, pendingGoogleUser: null });
          }
        }
      } catch (err) {
        console.error('Gagal mensinkronisasikan profil Firebase Auth:', err);
      }
    } else {
      restartAllSyncs((state: any) => usePaketStore.setState(state), null);
      usePaketStore.setState({ user: null, pendingGoogleUser: null });
    }
  });
}
