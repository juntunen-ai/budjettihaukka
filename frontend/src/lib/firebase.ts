import { initializeApp } from 'firebase/app';
import {
  GoogleAuthProvider,
  browserLocalPersistence,
  getAuth,
  getRedirectResult,
  onAuthStateChanged,
  setPersistence,
  signInWithPopup,
  signInWithRedirect,
  signOut,
  type User,
} from 'firebase/auth';

const firebaseConfig = {
  apiKey: import.meta.env.VITE_FIREBASE_API_KEY || 'AIzaSyBhlkjcfphoTfDIoviW2_dkDiEGQkSkoWk',
  authDomain: import.meta.env.VITE_FIREBASE_AUTH_DOMAIN || 'valtion-budjetti-data.firebaseapp.com',
  projectId: import.meta.env.VITE_FIREBASE_PROJECT_ID || 'valtion-budjetti-data',
  storageBucket: import.meta.env.VITE_FIREBASE_STORAGE_BUCKET || 'valtion-budjetti-data.firebasestorage.app',
  messagingSenderId: import.meta.env.VITE_FIREBASE_MESSAGING_SENDER_ID || '524401221130',
  appId: import.meta.env.VITE_FIREBASE_APP_ID || '1:524401221130:web:da159b39858c1d250989f3',
};

const firebaseApp = initializeApp(firebaseConfig);
const auth = getAuth(firebaseApp);
auth.languageCode = 'fi';

const googleProvider = new GoogleAuthProvider();
googleProvider.setCustomParameters({ prompt: 'select_account' });

export function observeAuthState(callback: (user: User | null) => void): () => void {
  return onAuthStateChanged(auth, callback);
}

export async function completeRedirectSignIn(): Promise<void> {
  await getRedirectResult(auth);
}

export async function signInWithGoogle(): Promise<void> {
  await setPersistence(auth, browserLocalPersistence);
  const useRedirect = window.matchMedia('(max-width: 700px)').matches;
  if (useRedirect) {
    await signInWithRedirect(auth, googleProvider);
    return;
  }

  try {
    await signInWithPopup(auth, googleProvider);
  } catch (error) {
    const code = typeof error === 'object' && error && 'code' in error ? String(error.code) : '';
    if (code === 'auth/popup-blocked') {
      await signInWithRedirect(auth, googleProvider);
      return;
    }
    throw error;
  }
}

export async function signOutUser(): Promise<void> {
  await signOut(auth);
}

export async function getAuthToken(): Promise<string> {
  const user = auth.currentUser;
  if (!user) {
    throw new Error('Google-kirjautuminen vaaditaan.');
  }
  return user.getIdToken();
}

export type { User };
