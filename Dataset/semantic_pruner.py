import os
import json
import torch
import argparse
import shutil
from PIL import Image
import torchvision.transforms as transforms
import torchvision.models as models
from tqdm import tqdm

class SemanticPruner:
    def __init__(self, threshold=0.95, device=None):
        """
        Initializes the Deep Embedding Pruner using ResNet18.
        """
        # Auto-detect hardware acceleration
        if device is None:
            self.device = 'cuda' if torch.cuda.is_available() else 'cpu'
        else:
            self.device = device
            
        self.threshold = threshold

        print(f"🧠 Loading ResNet18 Vision Encoder on {self.device.upper()}...")
        
        # Load pre-trained ResNet18 and remove the final classification layer
        # This gives us the pure 512-dimensional semantic feature vector
        weights = models.ResNet18_Weights.DEFAULT
        self.model = models.resnet18(weights=weights).eval().to(self.device)
        self.model.fc = torch.nn.Identity()

        # Standard ImageNet preprocessing pipeline
        self.preprocess = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ])

    def get_embedding(self, image_path):
        """Generates a dense semantic vector for a given image."""
        img = Image.open(image_path).convert('RGB')
        tensor = self.preprocess(img).unsqueeze(0).to(self.device)
        with torch.no_grad():
            embedding = self.model(tensor)
        return embedding

    def process_video_folder(self, video_folder):
        """Executes the Backward Content-Union Analysis using Embeddings."""
        metadata_path = os.path.join(video_folder, "extraction_metadata.json")
        
        if not os.path.exists(metadata_path):
            print(f"❌ Metadata not found in {video_folder}. Skipping.")
            return

        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        keyframes = metadata.get("keyframes", [])
        if not keyframes or len(keyframes) < 2:
            print(f"⚠️ Not enough keyframes to prune in {os.path.basename(video_folder)}.")
            return

        # Ensure frames are perfectly chronological before pruning
        keyframes = sorted(keyframes, key=lambda x: x["timestamp_seconds"])

        kept_frames = []
        pruned_frames = []

        print(f"\n✂️ Initiating Semantic Pruning for: {metadata.get('video', 'Unknown')}")

        # STEP 1: Establish the final frame as our first reference "Superset"
        reference_frame = keyframes[-1]
        kept_frames.append(reference_frame)
        ref_emb = self.get_embedding(os.path.join(video_folder, reference_frame["filename"]))

        # STEP 2: Iterate backward through time
        for i in tqdm(range(len(keyframes) - 2, -1, -1), desc="Analyzing Vectors"):
            current_frame = keyframes[i]
            frame_path = os.path.join(video_folder, current_frame["filename"])
            
            # Skip if the file was manually deleted earlier
            if not os.path.exists(frame_path):
                continue
                
            curr_emb = self.get_embedding(frame_path)

            # Compute Cosine Similarity between the vectors
            cos_sim = torch.nn.functional.cosine_similarity(ref_emb, curr_emb).item()

            if cos_sim >= self.threshold:
                # Similarity is high. The earlier frame is a subset of the later frame.
                pruned_frames.append(current_frame)
            else:
                # Similarity dropped! The board was erased or fundamentally changed.
                # Keep this frame, and make it the new reference superset for the frames before it.
                kept_frames.append(current_frame)
                reference_frame = current_frame
                ref_emb = curr_emb

        # STEP 3: Restore chronological order
        kept_frames.reverse()

        # STEP 4: Safely move redundant frames to a subfolder
        pruned_dir = os.path.join(video_folder, "pruned_redundant")
        os.makedirs(pruned_dir, exist_ok=True)

        for pf in pruned_frames:
            old_path = os.path.join(video_folder, pf["filename"])
            new_path = os.path.join(pruned_dir, pf["filename"])
            if os.path.exists(old_path):
                shutil.move(old_path, new_path)

        # STEP 5: Update the UniNote Dataset Metadata Schema
        metadata["labeling"] = {
            "enabled": True,
            "input_count": len(keyframes),
            "output_count": len(kept_frames),
            "pruned_count": len(pruned_frames),
            "method": "backward_semantic_embedding_resnet18"
        }
        metadata["keyframes_for_labeling"] = kept_frames

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=4)

        print(f"✅ Optimization Complete: Kept {len(kept_frames)} | Pruned {len(pruned_frames)}")


def main():
    parser = argparse.ArgumentParser(description="Deep Embedding Temporal Pruner for UniNote")
    parser.add_argument('--target_folder', type=str, help='Path to a specific video keyframe folder')
    parser.add_argument('--bulk_dir', type=str, help='Path to the root keyframes directory containing all video folders')
    parser.add_argument('--threshold', type=float, default=0.96, help='Cosine similarity threshold (default 0.96)')
    
    args = parser.parse_args()
    
    pruner = SemanticPruner(threshold=args.threshold)
    
    if args.target_folder:
        if os.path.exists(args.target_folder):
            pruner.process_video_folder(args.target_folder)
        else:
            print(f"❌ Folder not found: {args.target_folder}")
            
    elif args.bulk_dir:
        if os.path.exists(args.bulk_dir):
            subfolders = [os.path.join(args.bulk_dir, d) for d in os.listdir(args.bulk_dir) 
                          if os.path.isdir(os.path.join(args.bulk_dir, d))]
            
            print(f"🔍 Discovered {len(subfolders)} extracted video directories.")
            for folder in subfolders:
                pruner.process_video_folder(folder)
        else:
            print(f"❌ Directory not found: {args.bulk_dir}")
    else:
        print("⚠️ Please provide either --target_folder <path> or --bulk_dir <path>")


if __name__ == "__main__":
    main()